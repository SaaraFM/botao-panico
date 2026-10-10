package armazenamento_test

import (
	"context"
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
