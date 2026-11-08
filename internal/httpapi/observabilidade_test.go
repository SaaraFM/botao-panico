package httpapi_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/plataforma-mulher/botao-panico/internal/alerta"
	"github.com/plataforma-mulher/botao-panico/internal/httpapi"
	"github.com/plataforma-mulher/botao-panico/internal/observabilidade"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeServicoAlertas implementa a interface httpapi.ServicoAlertas para testes de API
type fakeServicoAlertas struct {
	alertaRetorno alerta.Alerta
	erroRetorno   error
}

func (f *fakeServicoAlertas) Acionar(ctx context.Context, pedido alerta.PedidoAlerta) (alerta.Alerta, error) {
	if f.erroRetorno != nil {
		return alerta.Alerta{}, f.erroRetorno
	}
	return f.alertaRetorno, nil
}

func (f *fakeServicoAlertas) Buscar(ctx context.Context, id string) (alerta.Alerta, error) {
	if f.erroRetorno != nil {
		return alerta.Alerta{}, f.erroRetorno
	}
	return f.alertaRetorno, nil
}

func (f *fakeServicoAlertas) Listar(ctx context.Context) ([]alerta.Alerta, error) {
	if f.erroRetorno != nil {
		return nil, f.erroRetorno
	}
	return []alerta.Alerta{f.alertaRetorno}, nil
}

func (f *fakeServicoAlertas) Instituicoes(ctx context.Context) ([]alerta.InstituicaoApoio, error) {
	return nil, nil
}

func TestHealthEndpoints(t *testing.T) {
	servico := &fakeServicoAlertas{}
	servidor := httpapi.NovoServidor(servico)
	h := servidor.Rotas()

	// 1. GET /health (legado / compatibilidade)
	req1 := httptest.NewRequest("GET", "/health", nil)
	rec1 := httptest.NewRecorder()
	h.ServeHTTP(rec1, req1)
	assert.Equal(t, http.StatusOK, rec1.Code)
	assert.JSONEq(t, `{"status":"ok"}`, rec1.Body.String())

	// 2. GET /healthz (liveness kubernetes / docker)
	req2 := httptest.NewRequest("GET", "/healthz", nil)
	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, req2)
	assert.Equal(t, http.StatusOK, rec2.Code)
	assert.JSONEq(t, `{"status":"ok"}`, rec2.Body.String())
}

func TestStatusReadiness(t *testing.T) {
	servico := &fakeServicoAlertas{}

	// Teste com componentes saudáveis
	optsSaudavel := httpapi.OpcoesServidor{
		VerificadorSaude: func(ctx context.Context) map[string]any {
			return map[string]any{
				"sqlite":   map[string]any{"status": "ok"},
				"rabbitmq": map[string]any{"status": "ok"},
			}
		},
	}
	srvSaudavel := httpapi.NovoServidorComOpcoes(servico, optsSaudavel)
	req1 := httptest.NewRequest("GET", "/status", nil)
	rec1 := httptest.NewRecorder()
	srvSaudavel.Rotas().ServeHTTP(rec1, req1)

	assert.Equal(t, http.StatusOK, rec1.Code)
	var resp1 map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &resp1))
	assert.Equal(t, "ok", resp1["status"])
	assert.Equal(t, "botao-panico", resp1["servico"])
	assert.NotEmpty(t, resp1["timestamp"])

	// Teste com componente degradado
	optsDegradado := httpapi.OpcoesServidor{
		VerificadorSaude: func(ctx context.Context) map[string]any {
			return map[string]any{
				"sqlite":   map[string]any{"status": "ok"},
				"rabbitmq": map[string]any{"status": "erro", "detalhe": "conexao amqp indisponivel"},
			}
		},
	}
	srvDegradado := httpapi.NovoServidorComOpcoes(servico, optsDegradado)
	req2 := httptest.NewRequest("GET", "/status", nil)
	rec2 := httptest.NewRecorder()
	srvDegradado.Rotas().ServeHTTP(rec2, req2)

	assert.Equal(t, http.StatusOK, rec2.Code)
	var resp2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp2))
	assert.Equal(t, "degradado", resp2["status"])
}

func TestCorrelationID_PropagacaoHeaders(t *testing.T) {
	servico := &fakeServicoAlertas{
		alertaRetorno: alerta.Alerta{ID: "alerta-123", UsuariaID: "usuaria-demo"},
	}
	srv := httpapi.NovoServidor(servico)
	h := srv.Rotas()

	// Caso 1: Correlation ID válido enviado pelo cliente
	corrCliente := "cliente-trace-xyz-987"
	req1 := httptest.NewRequest("GET", "/instituicoes", nil)
	req1.Header.Set("X-Correlation-ID", corrCliente)
	rec1 := httptest.NewRecorder()
	h.ServeHTTP(rec1, req1)

	assert.Equal(t, http.StatusOK, rec1.Code)
	assert.Equal(t, corrCliente, rec1.Header().Get("X-Correlation-ID"))
	assert.Equal(t, corrCliente, rec1.Header().Get("X-Request-ID"))

	// Caso 2: Nenhum cabeçalho enviado -> gera trace seguro
	req2 := httptest.NewRequest("GET", "/instituicoes", nil)
	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, req2)

	assert.Equal(t, http.StatusOK, rec2.Code)
	novoCorr := rec2.Header().Get("X-Correlation-ID")
	assert.True(t, strings.HasPrefix(novoCorr, "trace-"))
	assert.Equal(t, novoCorr, rec2.Header().Get("X-Request-ID"))

	// Caso 3: Correlation ID malicioso ou inválido com quebras de linha ou caracteres perigosos
	req3 := httptest.NewRequest("GET", "/instituicoes", nil)
	req3.Header.Set("X-Correlation-ID", "invalido\r\nInjecao: perigosa <script>")
	rec3 := httptest.NewRecorder()
	h.ServeHTTP(rec3, req3)

	assert.Equal(t, http.StatusOK, rec3.Code)
	sanitizado := rec3.Header().Get("X-Correlation-ID")
	assert.True(t, strings.HasPrefix(sanitizado, "trace-"), "deve descartar valor inseguro e gerar novo trace")
}

func TestMetricasEndpoint_SegurancaEFormatos(t *testing.T) {
	metricas := observabilidade.NovaMetricas()
	servico := &fakeServicoAlertas{}
	chave := "chave-secreta-de-servico-12345"

	// Servidor com autenticação exigida para acessos externos
	srv := httpapi.NovoServidorComOpcoes(servico, httpapi.OpcoesServidor{
		ChaveServico:                  chave,
		Metricas:                      metricas,
		MetricasAtivas:                true,
		MetricasAcessoLocalIrrestrito: true,
	})
	h := srv.Rotas()

	// 1. Acesso local irrestrito (localhost 127.0.0.1) -> 200 OK sem token
	reqLocal := httptest.NewRequest("GET", "/metrics", nil)
	reqLocal.RemoteAddr = "127.0.0.1:45678"
	recLocal := httptest.NewRecorder()
	h.ServeHTTP(recLocal, reqLocal)
	assert.Equal(t, http.StatusOK, recLocal.Code)
	assert.Contains(t, recLocal.Body.String(), "botao_panico_outbox_tarefas_pendentes")

	// 2. Acesso remoto externo sem credencial -> 401 Unauthorized
	reqRemotoSemAuth := httptest.NewRequest("GET", "/metrics", nil)
	reqRemotoSemAuth.RemoteAddr = "192.168.1.100:54321"
	recRemotoSemAuth := httptest.NewRecorder()
	h.ServeHTTP(recRemotoSemAuth, reqRemotoSemAuth)
	assert.Equal(t, http.StatusUnauthorized, recRemotoSemAuth.Code)
	assert.Contains(t, recRemotoSemAuth.Body.String(), "autenticação obrigatória")

	// 3. Acesso remoto externo com X-Service-Token válido -> 200 OK
	reqRemotoComToken := httptest.NewRequest("GET", "/metrics", nil)
	reqRemotoComToken.RemoteAddr = "192.168.1.100:54321"
	reqRemotoComToken.Header.Set("X-Service-Token", chave)
	recRemotoComToken := httptest.NewRecorder()
	h.ServeHTTP(recRemotoComToken, reqRemotoComToken)
	assert.Equal(t, http.StatusOK, recRemotoComToken.Code)
	assert.Contains(t, recRemotoComToken.Body.String(), "# HELP botao_panico_http_requests_total")

	// 4. Acesso remoto externo com Authorization: Bearer válido -> 200 OK
	reqRemotoComBearer := httptest.NewRequest("GET", "/metrics", nil)
	reqRemotoComBearer.RemoteAddr = "192.168.1.100:54321"
	reqRemotoComBearer.Header.Set("Authorization", "Bearer "+chave)
	recRemotoComBearer := httptest.NewRecorder()
	h.ServeHTTP(recRemotoComBearer, reqRemotoComBearer)
	assert.Equal(t, http.StatusOK, recRemotoComBearer.Code)

	// 5. Servidor com métricas desativadas -> 404 Not Found
	srvDesativado := httpapi.NovoServidorComOpcoes(servico, httpapi.OpcoesServidor{
		MetricasAtivas: false,
	})
	reqDesativado := httptest.NewRequest("GET", "/metrics", nil)
	recDesativado := httptest.NewRecorder()
	srvDesativado.Rotas().ServeHTTP(recDesativado, reqDesativado)
	assert.Equal(t, http.StatusNotFound, recDesativado.Code)
}

func TestMetricas_ProtecaoDadosSensiveis(t *testing.T) {
	metricas := observabilidade.NovaMetricas()
	servico := &fakeServicoAlertas{
		alertaRetorno: alerta.Alerta{
			ID:                 "alerta-super-secreto-999",
			UsuariaID:          "usuaria-vitima-confidencial",
			Mensagem:           "socorro estou em perigo na rua x",
			Local:     alerta.Localizacao{Latitude: -28.9346, Longitude: -49.4957},
		},
	}

	srv := httpapi.NovoServidorComOpcoes(servico, httpapi.OpcoesServidor{
		Metricas:                      metricas,
		MetricasAtivas:                true,
		MetricasAcessoLocalIrrestrito: true,
	})
	h := srv.Rotas()

	// Executa uma requisição a rota com ID específico
	req := httptest.NewRequest("GET", "/alertas/alerta-super-secreto-999", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	// Consulta o endpoint de métricas
	reqM := httptest.NewRequest("GET", "/metrics", nil)
	reqM.RemoteAddr = "127.0.0.1:50000"
	recM := httptest.NewRecorder()
	h.ServeHTTP(recM, reqM)

	corpoMetricas := recM.Body.String()

	// Valida que IDs dinâmicos, coordenadas, nomes ou credenciais NÃO estão nos rótulos
	assert.NotContains(t, corpoMetricas, "alerta-super-secreto-999")
	assert.NotContains(t, corpoMetricas, "usuaria-vitima-confidencial")
	assert.NotContains(t, corpoMetricas, "+5548999999999")
	assert.NotContains(t, corpoMetricas, "latitude")
	assert.NotContains(t, corpoMetricas, "longitude")

	// Garante que a rota foi agrupada corretamente sob o padrão :id
	assert.Contains(t, corpoMetricas, `rota="/alertas/:id"`)
}

// Garante que falha na coleta de métricas não impede a resposta HTTP
func TestMetricas_FalhaSilenciosaSemBloquearRequisicao(t *testing.T) {
	servico := &fakeServicoAlertas{
		alertaRetorno: alerta.Alerta{ID: "alerta-1"},
	}
	// Servidor sem objeto de métricas (nil)
	srv := httpapi.NovoServidorComOpcoes(servico, httpapi.OpcoesServidor{
		Metricas:                      nil,
		MetricasAtivas:                false,
		MetricasAcessoLocalIrrestrito: true,
	})

	req := httptest.NewRequest("GET", "/instituicoes", nil)
	rec := httptest.NewRecorder()
	srv.Rotas().ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
}
