package armazenamento

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/plataforma-mulher/botao-panico/internal/alerta"

	_ "modernc.org/sqlite" // driver SQLite em Go puro (não exige compilador C)
)

// esquema cria as tabelas próprias deste microserviço. "IF NOT EXISTS" torna
// a criação idempotente: pode rodar a cada inicialização sem perder dados.
const esquema = `
CREATE TABLE IF NOT EXISTS instituicoes (
	id        TEXT PRIMARY KEY,
	nome      TEXT NOT NULL,
	tipo      TEXT NOT NULL,
	telefone  TEXT NOT NULL,
	latitude  REAL NOT NULL,
	longitude REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS contatos_emergencia (
	id         TEXT PRIMARY KEY,
	usuaria_id TEXT NOT NULL,
	nome       TEXT NOT NULL,
	telefone   TEXT NOT NULL,
	email      TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_contatos_usuaria ON contatos_emergencia(usuaria_id);

CREATE TABLE IF NOT EXISTS alertas (
	id                  TEXT PRIMARY KEY,
	usuaria_id          TEXT NOT NULL,
	nivel               TEXT NOT NULL,
	mensagem            TEXT NOT NULL DEFAULT '',
	latitude            REAL NOT NULL,
	longitude           REAL NOT NULL,
	endereco_aproximado TEXT NOT NULL DEFAULT '',
	silencioso          INTEGER NOT NULL DEFAULT 0,
	status              TEXT NOT NULL,
	criado_em           TEXT NOT NULL,
	atualizado_em       TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS notificacoes (
	id         INTEGER PRIMARY KEY AUTOINCREMENT,
	alerta_id  TEXT NOT NULL REFERENCES alertas(id) ON DELETE CASCADE,
	destino    TEXT NOT NULL,
	canal      TEXT NOT NULL,
	sucesso    INTEGER NOT NULL,
	erro       TEXT NOT NULL DEFAULT '',
	duracao_ms INTEGER NOT NULL,
	enviado_em TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_notificacoes_alerta ON notificacoes(alerta_id);

CREATE TABLE IF NOT EXISTS alerta_instituicoes (
	alerta_id      TEXT NOT NULL REFERENCES alertas(id) ON DELETE CASCADE,
	instituicao_id TEXT NOT NULL REFERENCES instituicoes(id),
	PRIMARY KEY (alerta_id, instituicao_id)
);
`

// SQLite é o repositório persistente usado pela aplicação.
type SQLite struct {
	db *sql.DB
}

// AbrirSQLite abre (ou cria) o banco em dsn, aplica o esquema e os dados de
// exemplo. Exemplo de dsn: "file:botao_panico.db".
func AbrirSQLite(ctx context.Context, dsn string) (*SQLite, error) {
	db, err := sql.Open("sqlite", dsnComPragmas(dsn))
	if err != nil {
		return nil, fmt.Errorf("%w: %v", alerta.ErrBancoIndisponivel, err)
	}
	// SQLite aceita um escritor por vez; uma conexão evita "database is locked".
	db.SetMaxOpenConns(1)

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("%w: %v", alerta.ErrBancoIndisponivel, err)
	}

	s := &SQLite{db: db}
	if err := s.migrar(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

// Fechar encerra a conexão com o banco.
func (s *SQLite) Fechar() error {
	return s.db.Close()
}

// dsnComPragmas liga chaves estrangeiras e espera ocupado de 5s.
func dsnComPragmas(dsn string) string {
	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	return dsn + sep + "_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)"
}

// migrar cria o esquema e insere os dados de exemplo (INSERT OR IGNORE:
// idempotente, não duplica nem sobrescreve nada que já exista).
func (s *SQLite) migrar(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return classificar(alerta.ErrEscrita, err)
	}
	defer func() { _ = tx.Rollback() }() // sem efeito após Commit

	if _, err := tx.ExecContext(ctx, esquema); err != nil {
		return classificar(alerta.ErrEscrita, fmt.Errorf("criar esquema: %w", err))
	}
	for _, i := range instituicoesExemplo() {
		if _, err := tx.ExecContext(ctx,
			`INSERT OR IGNORE INTO instituicoes (id, nome, tipo, telefone, latitude, longitude) VALUES (?, ?, ?, ?, ?, ?)`,
			i.ID, i.Nome, i.Tipo, i.Telefone, i.Local.Latitude, i.Local.Longitude); err != nil {
			return classificar(alerta.ErrEscrita, fmt.Errorf("semear instituições: %w", err))
		}
	}
	for _, c := range contatosExemplo() {
		if _, err := tx.ExecContext(ctx,
			`INSERT OR IGNORE INTO contatos_emergencia (id, usuaria_id, nome, telefone, email) VALUES (?, ?, ?, ?, ?)`,
			c.ID, UsuariaDemo, c.Nome, c.Telefone, c.Email); err != nil {
			return classificar(alerta.ErrEscrita, fmt.Errorf("semear contatos: %w", err))
		}
	}
	if err := tx.Commit(); err != nil {
		return classificar(alerta.ErrEscrita, err)
	}
	return nil
}

// SalvarAlerta grava um alerta novo com notificações e instituições (transação).
func (s *SQLite) SalvarAlerta(ctx context.Context, a alerta.Alerta) error {
	return s.gravar(ctx, a, false)
}

// AtualizarAlerta substitui os dados de um alerta existente (transação):
// ou tudo é gravado, ou nada — sem atualizações parciais.
func (s *SQLite) AtualizarAlerta(ctx context.Context, a alerta.Alerta) error {
	return s.gravar(ctx, a, true)
}

func (s *SQLite) gravar(ctx context.Context, a alerta.Alerta, atualizar bool) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return classificar(alerta.ErrEscrita, err)
	}
	defer func() { _ = tx.Rollback() }()

	if atualizar {
		res, err := tx.ExecContext(ctx, `
			UPDATE alertas SET usuaria_id = ?, nivel = ?, mensagem = ?, latitude = ?, longitude = ?,
				endereco_aproximado = ?, silencioso = ?, status = ?, criado_em = ?, atualizado_em = ?
			WHERE id = ?`,
			a.UsuariaID, string(a.Nivel), a.Mensagem, a.Local.Latitude, a.Local.Longitude,
			a.EnderecoAproximado, a.Silencioso, string(a.Status), formatarData(a.CriadoEm), formatarData(a.AtualizadoEm), a.ID)
		if err != nil {
			return classificar(alerta.ErrEscrita, err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return classificar(alerta.ErrEscrita, err)
		}
		if n == 0 {
			return alerta.ErrNaoEncontrado
		}
		for _, q := range []string{`DELETE FROM notificacoes WHERE alerta_id = ?`, `DELETE FROM alerta_instituicoes WHERE alerta_id = ?`} {
			if _, err := tx.ExecContext(ctx, q, a.ID); err != nil {
				return classificar(alerta.ErrEscrita, err)
			}
		}
	} else {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO alertas (id, usuaria_id, nivel, mensagem, latitude, longitude,
				endereco_aproximado, silencioso, status, criado_em, atualizado_em)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			a.ID, a.UsuariaID, string(a.Nivel), a.Mensagem, a.Local.Latitude, a.Local.Longitude,
			a.EnderecoAproximado, a.Silencioso, string(a.Status), formatarData(a.CriadoEm), formatarData(a.AtualizadoEm)); err != nil {
			return classificar(alerta.ErrEscrita, err)
		}
	}

	for _, n := range a.Notificacoes {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO notificacoes (alerta_id, destino, canal, sucesso, erro, duracao_ms, enviado_em)
			VALUES (?, ?, ?, ?, ?, ?, ?)`,
			a.ID, n.Destino, n.Canal, n.Sucesso, n.Erro, n.DuracaoMs, n.EnviadoEm); err != nil {
			return classificar(alerta.ErrEscrita, err)
		}
	}
	for _, inst := range a.InstituicoesAlvo {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO alerta_instituicoes (alerta_id, instituicao_id) VALUES (?, ?)`, a.ID, inst.ID); err != nil {
			return classificar(alerta.ErrEscrita, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return classificar(alerta.ErrEscrita, err)
	}
	return nil
}

// BuscarAlerta devolve o alerta completo ou ErrNaoEncontrado.
func (s *SQLite) BuscarAlerta(ctx context.Context, id string) (alerta.Alerta, error) {
	linha := s.db.QueryRowContext(ctx, selecaoAlerta+` WHERE id = ?`, id)
	a, err := lerAlerta(linha)
	if errors.Is(err, sql.ErrNoRows) {
		return alerta.Alerta{}, alerta.ErrNaoEncontrado
	}
	if err != nil {
		return alerta.Alerta{}, classificar(alerta.ErrLeitura, err)
	}
	if err := s.carregarFilhos(ctx, &a); err != nil {
		return alerta.Alerta{}, err
	}
	return a, nil
}

// ListarAlertas devolve todos os alertas, do mais recente ao mais antigo.
func (s *SQLite) ListarAlertas(ctx context.Context) ([]alerta.Alerta, error) {
	linhas, err := s.db.QueryContext(ctx, selecaoAlerta+` ORDER BY criado_em DESC`)
	if err != nil {
		return nil, classificar(alerta.ErrLeitura, err)
	}
	defer linhas.Close()

	lista := []alerta.Alerta{}
	for linhas.Next() {
		a, err := lerAlerta(linhas)
		if err != nil {
			return nil, classificar(alerta.ErrLeitura, err)
		}
		lista = append(lista, a)
	}
	if err := linhas.Err(); err != nil {
		return nil, classificar(alerta.ErrLeitura, err)
	}
	// Fecha o cursor antes das consultas filhas (pool tem uma única conexão).
	if err := linhas.Close(); err != nil {
		return nil, classificar(alerta.ErrLeitura, err)
	}
	for i := range lista {
		if err := s.carregarFilhos(ctx, &lista[i]); err != nil {
			return nil, err
		}
	}
	return lista, nil
}

// ContatosDaUsuaria devolve os contatos de emergência da usuária.
func (s *SQLite) ContatosDaUsuaria(ctx context.Context, usuariaID string) ([]alerta.ContatoEmergencia, error) {
	linhas, err := s.db.QueryContext(ctx,
		`SELECT id, nome, telefone, email FROM contatos_emergencia WHERE usuaria_id = ? ORDER BY id`, usuariaID)
	if err != nil {
		return nil, classificar(alerta.ErrLeitura, err)
	}
	defer linhas.Close()

	contatos := []alerta.ContatoEmergencia{}
	for linhas.Next() {
		var c alerta.ContatoEmergencia
		if err := linhas.Scan(&c.ID, &c.Nome, &c.Telefone, &c.Email); err != nil {
			return nil, classificar(alerta.ErrLeitura, err)
		}
		contatos = append(contatos, c)
	}
	if err := linhas.Err(); err != nil {
		return nil, classificar(alerta.ErrLeitura, err)
	}
	return contatos, nil
}

// Instituicoes devolve a rede institucional de proteção.
func (s *SQLite) Instituicoes(ctx context.Context) ([]alerta.InstituicaoApoio, error) {
	return s.consultarInstituicoes(ctx,
		`SELECT id, nome, tipo, telefone, latitude, longitude FROM instituicoes ORDER BY id`)
}

const selecaoAlerta = `SELECT id, usuaria_id, nivel, mensagem, latitude, longitude,
	endereco_aproximado, silencioso, status, criado_em, atualizado_em FROM alertas`

// leitor é satisfeito por *sql.Row e *sql.Rows.
type leitor interface {
	Scan(dest ...any) error
}

func lerAlerta(l leitor) (alerta.Alerta, error) {
	var (
		a                      alerta.Alerta
		nivel, status          string
		criadoEm, atualizadoEm string
	)
	if err := l.Scan(&a.ID, &a.UsuariaID, &nivel, &a.Mensagem, &a.Local.Latitude, &a.Local.Longitude,
		&a.EnderecoAproximado, &a.Silencioso, &status, &criadoEm, &atualizadoEm); err != nil {
		return alerta.Alerta{}, err
	}
	a.Nivel = alerta.NivelRisco(nivel)
	a.Status = alerta.StatusAlerta(status)

	var err error
	if a.CriadoEm, err = time.Parse(time.RFC3339Nano, criadoEm); err != nil {
		return alerta.Alerta{}, fmt.Errorf("criado_em inválido: %w", err)
	}
	if a.AtualizadoEm, err = time.Parse(time.RFC3339Nano, atualizadoEm); err != nil {
		return alerta.Alerta{}, fmt.Errorf("atualizado_em inválido: %w", err)
	}
	return a, nil
}

// carregarFilhos preenche notificações e instituições-alvo do alerta.
func (s *SQLite) carregarFilhos(ctx context.Context, a *alerta.Alerta) error {
	linhas, err := s.db.QueryContext(ctx, `
		SELECT destino, canal, sucesso, erro, duracao_ms, enviado_em
		FROM notificacoes WHERE alerta_id = ? ORDER BY id`, a.ID)
	if err != nil {
		return classificar(alerta.ErrLeitura, err)
	}
	defer linhas.Close()

	a.Notificacoes = []alerta.ResultadoEnvio{}
	for linhas.Next() {
		var n alerta.ResultadoEnvio
		if err := linhas.Scan(&n.Destino, &n.Canal, &n.Sucesso, &n.Erro, &n.DuracaoMs, &n.EnviadoEm); err != nil {
			return classificar(alerta.ErrLeitura, err)
		}
		a.Notificacoes = append(a.Notificacoes, n)
	}
	if err := linhas.Err(); err != nil {
		return classificar(alerta.ErrLeitura, err)
	}
	if err := linhas.Close(); err != nil {
		return classificar(alerta.ErrLeitura, err)
	}

	insts, err := s.consultarInstituicoes(ctx, `
		SELECT i.id, i.nome, i.tipo, i.telefone, i.latitude, i.longitude
		FROM instituicoes i JOIN alerta_instituicoes ai ON ai.instituicao_id = i.id
		WHERE ai.alerta_id = ? ORDER BY i.id`, a.ID)
	if err != nil {
		return err
	}
	if len(insts) > 0 {
		a.InstituicoesAlvo = insts
	}
	return nil
}

func (s *SQLite) consultarInstituicoes(ctx context.Context, consulta string, args ...any) ([]alerta.InstituicaoApoio, error) {
	linhas, err := s.db.QueryContext(ctx, consulta, args...)
	if err != nil {
		return nil, classificar(alerta.ErrLeitura, err)
	}
	defer linhas.Close()

	lista := []alerta.InstituicaoApoio{}
	for linhas.Next() {
		var i alerta.InstituicaoApoio
		if err := linhas.Scan(&i.ID, &i.Nome, &i.Tipo, &i.Telefone, &i.Local.Latitude, &i.Local.Longitude); err != nil {
			return nil, classificar(alerta.ErrLeitura, err)
		}
		lista = append(lista, i)
	}
	if err := linhas.Err(); err != nil {
		return nil, classificar(alerta.ErrLeitura, err)
	}
	return lista, nil
}

func formatarData(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

// classificar embrulha o erro do driver no erro de domínio correto.
// Conexão fechada/indisponível vira ErrBancoIndisponivel; o resto vira
// o tipo da operação (ErrLeitura ou ErrEscrita). A mensagem original
// fica só no log — a camada HTTP nunca a expõe ao cliente.
func classificar(tipo error, err error) error {
	if errors.Is(err, alerta.ErrNaoEncontrado) {
		return err
	}
	if errors.Is(err, sql.ErrConnDone) || strings.Contains(err.Error(), "database is closed") {
		return fmt.Errorf("%w: %v", alerta.ErrBancoIndisponivel, err)
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return fmt.Errorf("%w: %w", tipo, err)
	}
	return fmt.Errorf("%w: %v", tipo, err)
}
