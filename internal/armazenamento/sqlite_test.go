package armazenamento_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/plataforma-mulher/botao-panico/internal/alerta"
	"github.com/plataforma-mulher/botao-panico/internal/armazenamento"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func dsnTemporario(t *testing.T) string {
	t.Helper()
	return "file:" + filepath.Join(t.TempDir(), "teste.db")
}

func abrir(t *testing.T, dsn string) *armazenamento.SQLite {
	t.Helper()
	repo, err := armazenamento.AbrirSQLite(context.Background(), dsn)
	require.NoError(t, err)
	return repo
}

func alertaExemplo(id string) alerta.Alerta {
	agora := time.Date(2026, 9, 10, 12, 0, 0, 123456789, time.UTC)
	return alerta.Alerta{
		ID: id, UsuariaID: armazenamento.UsuariaDemo, Nivel: alerta.RiscoAlto,
		Mensagem: "estou sendo seguida", Local: alerta.Localizacao{Latitude: -28.9346, Longitude: -49.4957},
		Status: alerta.StatusNotificando, CriadoEm: agora, AtualizadoEm: agora,
		ChaveIdempotencia: "chave-" + id, HashRequisicao: "hash-" + id,
		Notificacoes: []alerta.ResultadoEnvio{},
		InstituicoesAlvo: []alerta.InstituicaoApoio{
			{ID: "inst-1", Nome: "Polícia Militar — Araranguá", Tipo: "policia", Telefone: "190",
				Local: alerta.Localizacao{Latitude: -28.9346, Longitude: -49.4957}},
		},
	}
}

func TestSQLite_CriarEConsultarAlerta(t *testing.T) {
	ctx := context.Background()
	repo := abrir(t, dsnTemporario(t))
	t.Cleanup(func() { _ = repo.Fechar() })

	original := alertaExemplo("alerta-1")
	require.NoError(t, repo.SalvarAlerta(ctx, original))

	lido, err := repo.BuscarAlerta(ctx, "alerta-1")
	require.NoError(t, err)
	assert.Equal(t, original, lido)

	// Consulta por chave de idempotência
	porChave, err := repo.BuscarPorChaveIdempotencia(ctx, "chave-alerta-1")
	require.NoError(t, err)
	assert.Equal(t, original, porChave)
}

func TestSQLite_BuscarPorChaveInexistente(t *testing.T) {
	ctx := context.Background()
	repo := abrir(t, dsnTemporario(t))
	t.Cleanup(func() { _ = repo.Fechar() })

	_, err := repo.BuscarPorChaveIdempotencia(ctx, "nao-existe")
	assert.ErrorIs(t, err, alerta.ErrNaoEncontrado)
}

func TestSQLite_MigracaoSeguraDeBancoLegado(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "legado.db")
	dsn := "file:" + dbPath

	// Cria intencionalmente uma tabela 'alertas' no esquema legado (sem chave_idempotencia e hash_requisicao)
	db, err := sql.Open("sqlite", dsn)
	require.NoError(t, err)
	_, err = db.Exec(`
		CREATE TABLE alertas (
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
		CREATE TABLE notificacoes (
			id         INTEGER PRIMARY KEY AUTOINCREMENT,
			alerta_id  TEXT NOT NULL,
			destino    TEXT NOT NULL,
			canal      TEXT NOT NULL,
			sucesso    INTEGER NOT NULL,
			erro       TEXT NOT NULL DEFAULT '',
			duracao_ms INTEGER NOT NULL,
			enviado_em TEXT NOT NULL
		);
		CREATE TABLE alerta_instituicoes (
			alerta_id      TEXT NOT NULL,
			instituicao_id TEXT NOT NULL,
			PRIMARY KEY (alerta_id, instituicao_id)
		);
		INSERT INTO alertas (id, usuaria_id, nivel, mensagem, latitude, longitude, status, criado_em, atualizado_em)
		VALUES ('alerta-legado', 'usuaria-demo', 'alto', 'alerta antigo', -28.9, -49.4, 'concluido', '2026-09-01T10:00:00Z', '2026-09-01T10:00:00Z');
	`)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	// Ao abrir com AbrirSQLite, a migração não-destrutiva deve adicionar as novas colunas preservando os dados
	repo, err := armazenamento.AbrirSQLite(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = repo.Fechar() })

	// O registro legado existente deve continuar legível
	legado, err := repo.BuscarAlerta(ctx, "alerta-legado")
	require.NoError(t, err)
	assert.Equal(t, "alerta-legado", legado.ID)
	assert.Empty(t, legado.ChaveIdempotencia)

	// Novo alerta com chave de idempotência deve ser salvo normalmente
	novo := alertaExemplo("alerta-novo")
	require.NoError(t, repo.SalvarAlerta(ctx, novo))
	lidoNovo, err := repo.BuscarPorChaveIdempotencia(ctx, "chave-alerta-novo")
	require.NoError(t, err)
	assert.Equal(t, novo.ID, lidoNovo.ID)
}

func TestSQLite_PersisteAposReabrir(t *testing.T) {
	ctx := context.Background()
	dsn := dsnTemporario(t)

	repo := abrir(t, dsn)
	a := alertaExemplo("alerta-persistente")
	require.NoError(t, repo.SalvarAlerta(ctx, a))
	require.NoError(t, repo.Fechar())

	// Reabrir também reaplica o esquema e a semente: não pode duplicar nada.
	reaberto := abrir(t, dsn)
	t.Cleanup(func() { _ = reaberto.Fechar() })

	lido, err := reaberto.BuscarAlerta(ctx, a.ID)
	require.NoError(t, err)
	assert.Equal(t, a, lido)

	insts, err := reaberto.Instituicoes(ctx)
	require.NoError(t, err)
	assert.Len(t, insts, 4, "semente idempotente")
	contatos, err := reaberto.ContatosDaUsuaria(ctx, armazenamento.UsuariaDemo)
	require.NoError(t, err)
	assert.Len(t, contatos, 3)
}

func TestSQLite_RegistroInexistente(t *testing.T) {
	ctx := context.Background()
	repo := abrir(t, dsnTemporario(t))
	t.Cleanup(func() { _ = repo.Fechar() })

	_, err := repo.BuscarAlerta(ctx, "nao-existe")
	assert.ErrorIs(t, err, alerta.ErrNaoEncontrado)

	err = repo.AtualizarAlerta(ctx, alertaExemplo("nao-existe"))
	assert.ErrorIs(t, err, alerta.ErrNaoEncontrado)
}

func TestSQLite_AtualizacaoSubstituiNotificacoes(t *testing.T) {
	ctx := context.Background()
	repo := abrir(t, dsnTemporario(t))
	t.Cleanup(func() { _ = repo.Fechar() })

	a := alertaExemplo("alerta-2")
	require.NoError(t, repo.SalvarAlerta(ctx, a))

	a.Status = alerta.StatusConcluido
	a.EnderecoAproximado = "Centro, Araranguá, SC"
	a.AtualizadoEm = a.AtualizadoEm.Add(time.Second)
	a.Notificacoes = []alerta.ResultadoEnvio{
		{Destino: "Maria (mãe)", Canal: "sms", Sucesso: true, DuracaoMs: 120, EnviadoEm: "2026-09-10T12:00:01Z"},
		{Destino: "Ana (amiga)", Canal: "email", Sucesso: false, Erro: "caixa cheia", DuracaoMs: 200, EnviadoEm: "2026-09-10T12:00:01Z"},
	}
	require.NoError(t, repo.AtualizarAlerta(ctx, a))
	require.NoError(t, repo.AtualizarAlerta(ctx, a)) // repetir não duplica filhos

	lido, err := repo.BuscarAlerta(ctx, a.ID)
	require.NoError(t, err)
	assert.Equal(t, a, lido)

	lista, err := repo.ListarAlertas(ctx)
	require.NoError(t, err)
	require.Len(t, lista, 1)
	assert.Len(t, lista[0].Notificacoes, 2)
}

func TestSQLite_IDDuplicadoEFalhaDeEscrita(t *testing.T) {
	ctx := context.Background()
	repo := abrir(t, dsnTemporario(t))
	t.Cleanup(func() { _ = repo.Fechar() })

	require.NoError(t, repo.SalvarAlerta(ctx, alertaExemplo("dup")))
	err := repo.SalvarAlerta(ctx, alertaExemplo("dup"))
	assert.ErrorIs(t, err, alerta.ErrEscrita)
	assert.NotErrorIs(t, err, alerta.ErrNaoEncontrado)

	// Instituição inexistente viola a chave estrangeira: a transação inteira
	// é desfeita (o alerta não fica gravado pela metade).
	ruim := alertaExemplo("parcial")
	ruim.InstituicoesAlvo = []alerta.InstituicaoApoio{{ID: "inst-inexistente"}}
	assert.ErrorIs(t, repo.SalvarAlerta(ctx, ruim), alerta.ErrEscrita)
	_, err = repo.BuscarAlerta(ctx, "parcial")
	assert.ErrorIs(t, err, alerta.ErrNaoEncontrado, "rollback da transação")
}

func TestSQLite_BancoFechadoEhIndisponivel(t *testing.T) {
	ctx := context.Background()
	repo := abrir(t, dsnTemporario(t))
	require.NoError(t, repo.Fechar())

	_, err := repo.BuscarAlerta(ctx, "qualquer")
	assert.ErrorIs(t, err, alerta.ErrBancoIndisponivel)
	assert.NotErrorIs(t, err, alerta.ErrNaoEncontrado, "falha de infra não pode virar 404")

	_, err = repo.ListarAlertas(ctx)
	assert.ErrorIs(t, err, alerta.ErrBancoIndisponivel)

	assert.ErrorIs(t, repo.SalvarAlerta(ctx, alertaExemplo("x")), alerta.ErrBancoIndisponivel)
}

func TestSQLite_CaminhoInvalidoFalhaAoAbrir(t *testing.T) {
	_, err := armazenamento.AbrirSQLite(context.Background(), "file:/diretorio/que/nao/existe/x.db")
	assert.ErrorIs(t, err, alerta.ErrBancoIndisponivel)
}

func TestSQLite_ContextoCanceladoFalhaNaLeitura(t *testing.T) {
	repo := abrir(t, dsnTemporario(t))
	t.Cleanup(func() { _ = repo.Fechar() })

	ctx, cancelar := context.WithCancel(context.Background())
	cancelar()
	_, err := repo.ListarAlertas(ctx)
	assert.ErrorIs(t, err, alerta.ErrLeitura)
	assert.ErrorIs(t, err, context.Canceled)
}
