package armazenamento

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/plataforma-mulher/botao-panico/internal/alerta"
)

// SalvarAlertaComOutbox grava o alerta e a tarefa de mensageria na mesma transação SQLite (Transactional Outbox).
// Garante atomicidade: se o outbox falhar, o alerta sofre rollback e vice-versa.
func (s *SQLite) SalvarAlertaComOutbox(ctx context.Context, a alerta.Alerta, t alerta.TarefaOutbox) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return classificar(alerta.ErrEscrita, err)
	}
	defer tx.Rollback()

	// 1. Gravação do alerta dentro da transação
	const qAlerta = `
	INSERT INTO alertas (
		id, usuaria_id, nivel, mensagem, latitude, longitude,
		endereco_aproximado, silencioso, status, criado_em, atualizado_em,
		chave_idempotencia, hash_requisicao
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`

	silenciosoInt := 0
	if a.Silencioso {
		silenciosoInt = 1
	}

	_, err = tx.ExecContext(ctx, qAlerta,
		a.ID, a.UsuariaID, string(a.Nivel), a.Mensagem,
		a.Local.Latitude, a.Local.Longitude, a.EnderecoAproximado,
		silenciosoInt, string(a.Status),
		a.CriadoEm.Format(time.RFC3339Nano),
		a.AtualizadoEm.Format(time.RFC3339Nano),
		a.ChaveIdempotencia, a.HashRequisicao,
	)
	if err != nil {
		return classificar(alerta.ErrEscrita, err)
	}

	// 2. Gravação das instituições alvo se houver
	const qInst = `INSERT OR IGNORE INTO alerta_instituicoes (alerta_id, instituicao_id) VALUES (?, ?);`
	for _, inst := range a.InstituicoesAlvo {
		if _, err := tx.ExecContext(ctx, qInst, a.ID, inst.ID); err != nil {
			return classificar(alerta.ErrEscrita, err)
		}
	}

	// 3. Gravação da tarefa Outbox dentro da MESMA transação
	const qOutbox = `
	INSERT INTO tarefas_outbox (
		id, alerta_id, topico, payload_json, estado, tentativas,
		max_tentativas, proxima_tentativa_em, erro_ultimo, criado_em, atualizado_em
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`

	proximaTentativa := t.ProximaTentativaEm
	if proximaTentativa.IsZero() {
		proximaTentativa = time.Now()
	}

	_, err = tx.ExecContext(ctx, qOutbox,
		t.ID, t.AlertaID, t.Topico, t.PayloadJSON, string(t.Estado),
		t.Tentativas, t.MaxTentativas,
		proximaTentativa.Format(time.RFC3339Nano),
		t.ErroUltimo,
		t.CriadoEm.Format(time.RFC3339Nano),
		t.AtualizadoEm.Format(time.RFC3339Nano),
	)
	if err != nil {
		return classificar(alerta.ErrEscrita, err)
	}

	if err := tx.Commit(); err != nil {
		return classificar(alerta.ErrEscrita, err)
	}
	return nil
}

// BuscarTarefasPendentes retorna as tarefas prontas para publicação pelo Relay.
func (s *SQLite) BuscarTarefasPendentes(ctx context.Context, limite int) ([]alerta.TarefaOutbox, error) {
	const q = `
	SELECT id, alerta_id, topico, payload_json, estado, tentativas,
	       max_tentativas, proxima_tentativa_em, erro_ultimo, criado_em, atualizado_em
	FROM tarefas_outbox
	WHERE estado IN ('pendente') AND proxima_tentativa_em <= ?
	ORDER BY criado_em ASC
	LIMIT ?;`

	if limite <= 0 {
		limite = 50
	}

	rows, err := s.db.QueryContext(ctx, q, time.Now().Format(time.RFC3339Nano), limite)
	if err != nil {
		return nil, classificar(alerta.ErrLeitura, err)
	}
	defer rows.Close()

	var tarefas []alerta.TarefaOutbox
	for rows.Next() {
		var t alerta.TarefaOutbox
		var estadoStr, proxStr, criadoStr, atualizadoStr string

		err := rows.Scan(
			&t.ID, &t.AlertaID, &t.Topico, &t.PayloadJSON, &estadoStr,
			&t.Tentativas, &t.MaxTentativas, &proxStr, &t.ErroUltimo,
			&criadoStr, &atualizadoStr,
		)
		if err != nil {
			return nil, classificar(alerta.ErrLeitura, err)
		}

		t.Estado = alerta.EstadoTarefa(estadoStr)
		t.ProximaTentativaEm, _ = time.Parse(time.RFC3339Nano, proxStr)
		t.CriadoEm, _ = time.Parse(time.RFC3339Nano, criadoStr)
		t.AtualizadoEm, _ = time.Parse(time.RFC3339Nano, atualizadoStr)

		tarefas = append(tarefas, t)
	}

	return tarefas, rows.Err()
}

// MarcarTarefaPublicada atualiza o estado para 'publicado' após envio ao broker.
func (s *SQLite) MarcarTarefaPublicada(ctx context.Context, id string) error {
	const q = `UPDATE tarefas_outbox SET estado = 'publicado', atualizado_em = ? WHERE id = ?;`
	res, err := s.db.ExecContext(ctx, q, time.Now().Format(time.RFC3339Nano), id)
	if err != nil {
		return classificar(alerta.ErrEscrita, err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return alerta.ErrNaoEncontrado
	}
	return nil
}

// MarcarTarefaConcluida marca a tarefa como 'concluido' após o processamento bem-sucedido no worker.
func (s *SQLite) MarcarTarefaConcluida(ctx context.Context, id string) error {
	const q = `UPDATE tarefas_outbox SET estado = 'concluido', atualizado_em = ? WHERE id = ?;`
	res, err := s.db.ExecContext(ctx, q, time.Now().Format(time.RFC3339Nano), id)
	if err != nil {
		return classificar(alerta.ErrEscrita, err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return alerta.ErrNaoEncontrado
	}
	return nil
}

// IncrementarFalhaTarefa registra tentativa falha e programa backoff ou marca 'falha'.
func (s *SQLite) IncrementarFalhaTarefa(ctx context.Context, id string, erroMsg string, proximaTentativa time.Time) error {
	const q = `
	UPDATE tarefas_outbox
	SET tentativas = tentativas + 1,
	    erro_ultimo = ?,
	    proxima_tentativa_em = ?,
	    estado = CASE WHEN tentativas + 1 >= max_tentativas THEN 'falha' ELSE 'pendente' END,
	    atualizado_em = ?
	WHERE id = ?;`

	res, err := s.db.ExecContext(ctx, q,
		erroMsg,
		proximaTentativa.Format(time.RFC3339Nano),
		time.Now().Format(time.RFC3339Nano),
		id,
	)
	if err != nil {
		return classificar(alerta.ErrEscrita, err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return alerta.ErrNaoEncontrado
	}
	return nil
}

// BuscarTarefaPorID consulta uma tarefa outbox por ID.
func (s *SQLite) BuscarTarefaPorID(ctx context.Context, id string) (alerta.TarefaOutbox, error) {
	const q = `
	SELECT id, alerta_id, topico, payload_json, estado, tentativas,
	       max_tentativas, proxima_tentativa_em, erro_ultimo, criado_em, atualizado_em
	FROM tarefas_outbox
	WHERE id = ?;`

	var t alerta.TarefaOutbox
	var estadoStr, proxStr, criadoStr, atualizadoStr string

	err := s.db.QueryRowContext(ctx, q, id).Scan(
		&t.ID, &t.AlertaID, &t.Topico, &t.PayloadJSON, &estadoStr,
		&t.Tentativas, &t.MaxTentativas, &proxStr, &t.ErroUltimo,
		&criadoStr, &atualizadoStr,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return alerta.TarefaOutbox{}, alerta.ErrNaoEncontrado
	}
	if err != nil {
		return alerta.TarefaOutbox{}, classificar(alerta.ErrLeitura, err)
	}

	t.Estado = alerta.EstadoTarefa(estadoStr)
	t.ProximaTentativaEm, _ = time.Parse(time.RFC3339Nano, proxStr)
	t.CriadoEm, _ = time.Parse(time.RFC3339Nano, criadoStr)
	t.AtualizadoEm, _ = time.Parse(time.RFC3339Nano, atualizadoStr)

	return t, nil
}

// BuscarTarefaPorAlertaID consulta a tarefa outbox vinculada a um alerta específico.
func (s *SQLite) BuscarTarefaPorAlertaID(ctx context.Context, alertaID string) (alerta.TarefaOutbox, error) {
	const q = `
	SELECT id, alerta_id, topico, payload_json, estado, tentativas,
	       max_tentativas, proxima_tentativa_em, erro_ultimo, criado_em, atualizado_em
	FROM tarefas_outbox
	WHERE alerta_id = ?
	ORDER BY criado_em DESC
	LIMIT 1;`

	var t alerta.TarefaOutbox
	var estadoStr, proxStr, criadoStr, atualizadoStr string

	err := s.db.QueryRowContext(ctx, q, alertaID).Scan(
		&t.ID, &t.AlertaID, &t.Topico, &t.PayloadJSON, &estadoStr,
		&t.Tentativas, &t.MaxTentativas, &proxStr, &t.ErroUltimo,
		&criadoStr, &atualizadoStr,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return alerta.TarefaOutbox{}, alerta.ErrNaoEncontrado
	}
	if err != nil {
		return alerta.TarefaOutbox{}, classificar(alerta.ErrLeitura, err)
	}

	t.Estado = alerta.EstadoTarefa(estadoStr)
	t.ProximaTentativaEm, _ = time.Parse(time.RFC3339Nano, proxStr)
	t.CriadoEm, _ = time.Parse(time.RFC3339Nano, criadoStr)
	t.AtualizadoEm, _ = time.Parse(time.RFC3339Nano, atualizadoStr)

	return t, nil
}
