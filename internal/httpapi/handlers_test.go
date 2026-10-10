package httpapi_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/plataforma-mulher/botao-panico/internal/alerta"
	"github.com/plataforma-mulher/botao-panico/internal/armazenamento"
	"github.com/plataforma-mulher/botao-panico/internal/httpapi"
	"github.com/plataforma-mulher/botao-panico/internal/notificacao"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const pedidoValido = `{"usuaria_id":"usuaria-demo","nivel":"alto","mensagem":"socorro","local":{"latitude":-28.9346,"longitude":-49.4957}}`

// notificadorInstantaneo evita a latência simulada nos testes da API.
type notificadorInstantaneo struct{}

func (notificadorInstantaneo) Canal() string                                { return "sms" }
func (notificadorInstantaneo) Enviar(context.Context, string, string) error { return nil }

// novoServidorReal monta a pilha completa com SQLite temporário.
func novoServidorReal(t *testing.T) http.Handler {
	t.Helper()
	repo, err := armazenamento.AbrirSQLite(context.Background(), "file:"+filepath.Join(t.TempDir(), "api.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = repo.Fechar() })
	servico := alerta.NovoServico(repo, notificacao.NovoDespachante(notificadorInstantaneo{}), nil, time.Second)
	return httpapi.NovoServidor(servico).Rotas()
}

func requisitar(h http.Handler, metodo, rota, corpo string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(metodo, rota, strings.NewReader(corpo))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func decodificarErro(t *testing.T, rec *httptest.ResponseRecorder) httpapi.RespostaErro {
	t.Helper()
	var r httpapi.RespostaErro
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &r))
	return r
}

func TestAPI_CriarConsultarEListar(t *testing.T) {
	h := novoServidorReal(t)

	rec := requisitar(h, http.MethodPost, "/alertas", pedidoValido)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	var criado alerta.Alerta
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &criado))
	assert.Equal(t, alerta.StatusConcluido, criado.Status)
	assert.Len(t, criado.Notificacoes, 7)

	rec = requisitar(h, http.MethodGet, "/alertas/"+criado.ID, "")
	require.Equal(t, http.StatusOK, rec.Code)
	var lido alerta.Alerta
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &lido))
	assert.Equal(t, criado.ID, lido.ID)
	assert.Len(t, lido.Notificacoes, 7)

	rec = requisitar(h, http.MethodGet, "/alertas", "")
	require.Equal(t, http.StatusOK, rec.Code)
	var lista []alerta.Alerta
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &lista))
	require.Len(t, lista, 1)
	assert.Equal(t, criado.ID, lista[0].ID)
}

func TestAPI_HealthEInstituicoes(t *testing.T) {
	h := novoServidorReal(t)
	assert.Equal(t, http.StatusOK, requisitar(h, http.MethodGet, "/health", "").Code)

	rec := requisitar(h, http.MethodGet, "/instituicoes", "")
	require.Equal(t, http.StatusOK, rec.Code)
	var insts []alerta.InstituicaoApoio
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &insts))
	assert.Len(t, insts, 4)
}

func TestAPI_AlertaInexistente404(t *testing.T) {
	rec := requisitar(novoServidorReal(t), http.MethodGet, "/alertas/nao-existe", "")
	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.Equal(t, httpapi.CodigoNaoEncontrado, decodificarErro(t, rec).Codigo)
}

func TestAPI_ErrosDeEntrada(t *testing.T) {
	casos := []struct {
		nome   string
		corpo  string
		status int
		codigo string
	}{
		{"JSON malformado", `{"nivel":`, http.StatusBadRequest, httpapi.CodigoJSONInvalido},
		{"corpo vazio", ``, http.StatusBadRequest, httpapi.CodigoJSONInvalido},
		{"campo desconhecido", `{"usuaria_id":"u","hacker":true}`, http.StatusBadRequest, httpapi.CodigoJSONInvalido},
		{"tipo errado", `{"usuaria_id":123}`, http.StatusBadRequest, httpapi.CodigoJSONInvalido},
		{"dois objetos", pedidoValido + pedidoValido, http.StatusBadRequest, httpapi.CodigoJSONInvalido},
		{"corpo gigante", `{"mensagem":"` + strings.Repeat("a", 70<<10) + `"}`, http.StatusRequestEntityTooLarge, httpapi.CodigoCorpoGrande},
		{"validação", `{"nivel":"urgente"}`, http.StatusUnprocessableEntity, httpapi.CodigoDadosInvalidos},
	}
	h := novoServidorReal(t)
	for _, c := range casos {
		t.Run(c.nome, func(t *testing.T) {
			rec := requisitar(h, http.MethodPost, "/alertas", c.corpo)
			assert.Equal(t, c.status, rec.Code)
			assert.Equal(t, c.codigo, decodificarErro(t, rec).Codigo)
		})
	}
}

func TestAPI_ValidacaoMantemCamposPorCampo(t *testing.T) {
	rec := requisitar(novoServidorReal(t), http.MethodPost, "/alertas", `{"nivel":"urgente"}`)
	require.Equal(t, http.StatusUnprocessableEntity, rec.Code)
	r := decodificarErro(t, rec)
	assert.Equal(t, "dados inválidos", r.Erro)
	assert.Contains(t, r.Campos, "usuaria_id")
	assert.Contains(t, r.Campos, "nivel")
	assert.Contains(t, r.Campos, "local")
}

// servicoFalho é um fake de ServicoAlertas que sempre devolve o erro configurado.
type servicoFalho struct{ err error }

func (s servicoFalho) Acionar(context.Context, alerta.PedidoAlerta) (alerta.Alerta, error) {
	return alerta.Alerta{}, s.err
}
func (s servicoFalho) Buscar(context.Context, string) (alerta.Alerta, error) {
	return alerta.Alerta{}, s.err
}
func (s servicoFalho) Listar(context.Context) ([]alerta.Alerta, error) { return nil, s.err }
func (s servicoFalho) Instituicoes(context.Context) ([]alerta.InstituicaoApoio, error) {
	return nil, s.err
}

func TestAPI_ErrosDeInfraestrutura(t *testing.T) {
	segredo := "SQL logic error: no such table alertas"
	casos := []struct {
		nome   string
		err    error
		status int
		codigo string
	}{
		{"banco indisponível", fmt.Errorf("%w: %s", alerta.ErrBancoIndisponivel, segredo), http.StatusServiceUnavailable, httpapi.CodigoBancoIndisponivel},
		{"falha de leitura", fmt.Errorf("%w: %s", alerta.ErrLeitura, segredo), http.StatusInternalServerError, httpapi.CodigoErroInterno},
		{"falha de escrita", fmt.Errorf("%w: %s", alerta.ErrEscrita, segredo), http.StatusInternalServerError, httpapi.CodigoErroInterno},
		{"timeout", fmt.Errorf("x: %w", context.DeadlineExceeded), http.StatusServiceUnavailable, httpapi.CodigoTempoEsgotado},
		{"inesperado", fmt.Errorf("%s", segredo), http.StatusInternalServerError, httpapi.CodigoErroInterno},
	}
	for _, c := range casos {
		t.Run(c.nome, func(t *testing.T) {
			h := httpapi.NovoServidor(servicoFalho{err: c.err}).Rotas()
			for _, r := range []struct{ metodo, rota, corpo string }{
				{http.MethodPost, "/alertas", pedidoValido},
				{http.MethodGet, "/alertas", ""},
				{http.MethodGet, "/alertas/abc", ""},
				{http.MethodGet, "/instituicoes", ""},
			} {
				rec := requisitar(h, r.metodo, r.rota, r.corpo)
				assert.Equal(t, c.status, rec.Code, r.rota)
				assert.Equal(t, c.codigo, decodificarErro(t, rec).Codigo)
				assert.NotContains(t, rec.Body.String(), "SQL", "detalhes internos não vazam")
			}
		})
	}
}

func TestAPI_BancoFechadoNaoRetorna404(t *testing.T) {
	repo, err := armazenamento.AbrirSQLite(context.Background(), "file:"+filepath.Join(t.TempDir(), "x.db"))
	require.NoError(t, err)
	servico := alerta.NovoServico(repo, notificacao.NovoDespachante(notificadorInstantaneo{}), nil, time.Second)
	h := httpapi.NovoServidor(servico).Rotas()
	require.NoError(t, repo.Fechar())

	rec := requisitar(h, http.MethodGet, "/alertas/qualquer", "")
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.Equal(t, httpapi.CodigoBancoIndisponivel, decodificarErro(t, rec).Codigo)
}
