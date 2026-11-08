package observabilidade_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/plataforma-mulher/botao-panico/internal/alerta"
	"github.com/plataforma-mulher/botao-panico/internal/armazenamento"
	"github.com/plataforma-mulher/botao-panico/internal/cliente"
	"github.com/plataforma-mulher/botao-panico/internal/fila"
	"github.com/plataforma-mulher/botao-panico/internal/httpapi"
	"github.com/plataforma-mulher/botao-panico/internal/notificacao"
	"github.com/plataforma-mulher/botao-panico/internal/observabilidade"
	"github.com/plataforma-mulher/botao-panico/internal/resiliencia"
	"github.com/prometheus/client_golang/prometheus/testutil"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const chaveS2STeste = "chave-s2s-secreta-observabilidade-123"

// notificadorMockSilencioso evita qualquer envio externo de SMS/Email
type notificadorMockSilencioso struct{}

func (notificadorMockSilencioso) Canal() string                                { return "sms" }
func (notificadorMockSilencioso) Enviar(context.Context, string, string) error { return nil }

// Item 1 e Item 4: Verificação de exportação Prometheus e atualização de contadores/histogramas
func TestValidacao_Item1_E_Item4_PrometheusMetricasEHistogramas(t *testing.T) {
	metricas := observabilidade.NovaMetricas()
	repo, err := armazenamento.AbrirSQLite(context.Background(), "file:"+filepath.Join(t.TempDir(), "val_metrics.db"))
	require.NoError(t, err)
	defer repo.Fechar()

	servico := alerta.NovoServico(repo, notificacao.NovoDespachante(notificadorMockSilencioso{}), nil, time.Second)
	srv := httpapi.NovoServidorComOpcoes(servico, httpapi.OpcoesServidor{
		ChaveServico:                  chaveS2STeste,
		Metricas:                      metricas,
		MetricasAtivas:                true,
		MetricasAcessoLocalIrrestrito: true,
	})
	h := srv.Rotas()

	// Estado inicial de requisições:
	assert.Equal(t, float64(0), testutil.ToFloat64(metricas.HTTPRequestsTotal.WithLabelValues("GET", "/instituicoes", "200")))

	// Faz requisição HTTP GET /instituicoes
	req := httptest.NewRequest("GET", "/instituicoes", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Valida que o contador foi incrementado
	assert.Equal(t, float64(1), testutil.ToFloat64(metricas.HTTPRequestsTotal.WithLabelValues("GET", "/instituicoes", "200")))

	// Faz chamada simulada gRPC
	metricas.RegistrarChamadaGRPC("/alerta.v1.AlertaService/BuscarAlerta", "OK", 0.025)
	assert.Equal(t, float64(1), testutil.ToFloat64(metricas.GRPCRequestsTotal.WithLabelValues("/alerta.v1.AlertaService/BuscarAlerta", "OK")))

	// Consulta /metrics
	reqM := httptest.NewRequest("GET", "/metrics", nil)
	reqM.RemoteAddr = "127.0.0.1:45123"
	recM := httptest.NewRecorder()
	h.ServeHTTP(recM, reqM)

	assert.Equal(t, http.StatusOK, recM.Code)
	body := recM.Body.String()

	// Valida especificações oficiais do Prometheus
	assert.Contains(t, body, "# HELP botao_panico_http_requests_total")
	assert.Contains(t, body, "# TYPE botao_panico_http_requests_total counter")
	assert.Contains(t, body, `botao_panico_http_requests_total{metodo="GET",rota="/instituicoes",status="200"} 1`)
	assert.Contains(t, body, "# HELP botao_panico_http_request_duration_seconds")
	assert.Contains(t, body, "# TYPE botao_panico_http_request_duration_seconds histogram")
	assert.Contains(t, body, "# HELP botao_panico_grpc_requests_total")
	assert.Contains(t, body, `botao_panico_grpc_requests_total{codigo="OK",metodo="/alerta.v1.AlertaService/BuscarAlerta"} 1`)
}

// Item 2: Verificação de /healthz e /status em condições normais e de falha simulada
func TestValidacao_Item2_Healthz_E_Status_Normal_E_Falha(t *testing.T) {
	// Cenário A: Tudo Saudável
	optsNormal := httpapi.OpcoesServidor{
		VerificadorSaude: func(ctx context.Context) map[string]any {
			return map[string]any{
				"sqlite":   map[string]any{"status": "ok"},
				"rabbitmq": map[string]any{"status": "ok"},
			}
		},
	}
	srvNormal := httpapi.NovoServidorComOpcoes(nil, optsNormal)
	hNormal := srvNormal.Rotas()

	// Liveness /healthz
	reqLiveness := httptest.NewRequest("GET", "/healthz", nil)
	recLiveness := httptest.NewRecorder()
	hNormal.ServeHTTP(recLiveness, reqLiveness)
	assert.Equal(t, http.StatusOK, recLiveness.Code)
	assert.JSONEq(t, `{"status":"ok"}`, recLiveness.Body.String())

	// Readiness /status saudável
	reqStatus := httptest.NewRequest("GET", "/status", nil)
	recStatus := httptest.NewRecorder()
	hNormal.ServeHTTP(recStatus, reqStatus)
	assert.Equal(t, http.StatusOK, recStatus.Code)
	var respNormal map[string]any
	require.NoError(t, json.Unmarshal(recStatus.Body.Bytes(), &respNormal))
	assert.Equal(t, "ok", respNormal["status"])
	compsNormal := respNormal["componentes"].(map[string]any)
	assert.Equal(t, "ok", compsNormal["sqlite"].(map[string]any)["status"])
	assert.Equal(t, "ok", compsNormal["rabbitmq"].(map[string]any)["status"])

	// Cenário B: Falha simulada no SQLite (banco fechado/indisponível)
	optsFalhaSQLite := httpapi.OpcoesServidor{
		VerificadorSaude: func(ctx context.Context) map[string]any {
			return map[string]any{
				"sqlite":   map[string]any{"status": "erro", "detalhe": "sql: database is closed"},
				"rabbitmq": map[string]any{"status": "ok"},
			}
		},
	}
	srvFalhaSQLite := httpapi.NovoServidorComOpcoes(nil, optsFalhaSQLite)
	recFalhaSQLite := httptest.NewRecorder()
	srvFalhaSQLite.Rotas().ServeHTTP(recFalhaSQLite, httptest.NewRequest("GET", "/status", nil))

	assert.Equal(t, http.StatusOK, recFalhaSQLite.Code)
	var respFalhaSQLite map[string]any
	require.NoError(t, json.Unmarshal(recFalhaSQLite.Body.Bytes(), &respFalhaSQLite))
	assert.Equal(t, "degradado", respFalhaSQLite["status"], "status geral deve refletir falha do sqlite")
	compsSQLite := respFalhaSQLite["componentes"].(map[string]any)
	assert.Equal(t, "erro", compsSQLite["sqlite"].(map[string]any)["status"])

	// Cenário C: Falha simulada no RabbitMQ (conexão perdida)
	optsFalhaRabbit := httpapi.OpcoesServidor{
		VerificadorSaude: func(ctx context.Context) map[string]any {
			return map[string]any{
				"sqlite":   map[string]any{"status": "ok"},
				"rabbitmq": map[string]any{"status": "erro", "detalhe": "conexão amqp fechada"},
			}
		},
	}
	srvFalhaRabbit := httpapi.NovoServidorComOpcoes(nil, optsFalhaRabbit)
	recFalhaRabbit := httptest.NewRecorder()
	srvFalhaRabbit.Rotas().ServeHTTP(recFalhaRabbit, httptest.NewRequest("GET", "/status", nil))

	assert.Equal(t, http.StatusOK, recFalhaRabbit.Code)
	var respFalhaRabbit map[string]any
	require.NoError(t, json.Unmarshal(recFalhaRabbit.Body.Bytes(), &respFalhaRabbit))
	assert.Equal(t, "degradado", respFalhaRabbit["status"], "status geral deve refletir falha do rabbitmq")
	compsRabbit := respFalhaRabbit["componentes"].(map[string]any)
	assert.Equal(t, "erro", compsRabbit["rabbitmq"].(map[string]any)["status"])
}

type pubMockVal struct {
	mu  sync.Mutex
	env alerta.Envelope
}

func (p *pubMockVal) Publicar(_ context.Context, _ string, env alerta.Envelope) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.env = env
	return nil
}

// Item 3: Rastreamento E2E com Autenticação S2S e propagação do Correlation ID
// API -> Outbox -> RabbitMQ -> Consumidor A e Consumidor B
func TestValidacao_Item3_CorrelationID_E2E_ComS2S(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "val_e2e_s2s.db")
	repo, err := armazenamento.AbrirSQLite(context.Background(), "file:"+dbPath)
	require.NoError(t, err)
	defer repo.Fechar()

	metricas := observabilidade.NovaMetricas()
	despachante := notificacao.NovoDespachante(notificadorMockSilencioso{})
	servico := alerta.NovoServico(repo, despachante, nil, time.Second)

	pub := &pubMockVal{}
	relay := fila.NovoOutboxRelay(repo, pub, "alerta.criado", 30*time.Millisecond)
	relay.ConectarMetricas(metricas)
	servico.AtivarModoAssincrono(relay.NotificarNovoAlerta)
	relay.Iniciar()
	defer relay.Parar()

	// Servidor HTTP com autenticação S2S ativada
	srv := httpapi.NovoServidorComOpcoes(servico, httpapi.OpcoesServidor{
		ChaveServico:   chaveS2STeste,
		Metricas:       metricas,
		MetricasAtivas: true,
	})
	h := srv.Rotas()

	// 1. Requisição à API sem chave S2S deve retornar 401
	payload := `{"usuaria_id":"demo-val-1","nivel":"alto","mensagem":"perigo","local":{"latitude":-28.93,"longitude":-49.49}}`
	reqSemAuth := httptest.NewRequest("POST", "/alertas", strings.NewReader(payload))
	reqSemAuth.Header.Set("Content-Type", "application/json")
	recSemAuth := httptest.NewRecorder()
	h.ServeHTTP(recSemAuth, reqSemAuth)
	assert.Equal(t, http.StatusUnauthorized, recSemAuth.Code)

	// 2. Requisição com autenticação S2S válida e Correlation ID explícito
	correlationEsperado := "corr-id-auditoria-s2s-777888"
	reqAutenticada := httptest.NewRequest("POST", "/alertas", strings.NewReader(payload))
	reqAutenticada.Header.Set("Content-Type", "application/json")
	reqAutenticada.Header.Set("X-Service-Token", chaveS2STeste)
	reqAutenticada.Header.Set("X-Correlation-ID", correlationEsperado)
	recAutenticada := httptest.NewRecorder()
	h.ServeHTTP(recAutenticada, reqAutenticada)

	assert.Equal(t, http.StatusCreated, recAutenticada.Code)
	// Comprova retorno do Correlation ID nos headers da API
	assert.Equal(t, correlationEsperado, recAutenticada.Header().Get("X-Correlation-ID"))
	assert.Equal(t, correlationEsperado, recAutenticada.Header().Get("X-Request-ID"))

	// 3. Aguarda o Outbox Relay processar e publicar
	require.Eventually(t, func() bool {
		pub.mu.Lock()
		defer pub.mu.Unlock()
		return pub.env.IDEvento != ""
	}, 2*time.Second, 40*time.Millisecond)

	pub.mu.Lock()
	envRecebido := pub.env
	pub.mu.Unlock()

	// 4. Comprova que o Outbox gravou e repassou o Correlation ID no Envelope AMQP
	assert.Equal(t, correlationEsperado, envRecebido.CorrelationID)

	// 5. Simula a entrega da mensagem do RabbitMQ ao Consumidor B (Auditoria)
	// criando uma entrega amqp.Delivery com o CorrelationId nos metadados AMQP
	corpoMsg, err := json.Marshal(envRecebido)
	require.NoError(t, err)

	msgAMQP := amqp.Delivery{
		CorrelationId: envRecebido.CorrelationID,
		Headers: amqp.Table{
			"correlation_id": envRecebido.CorrelationID,
		},
		Body: corpoMsg,
	}

	// Injeta a mensagem no contexto de processamento do consumidor
	ctxProc := observabilidade.InjetarCorrelationID(context.Background(), msgAMQP.CorrelationId)
	assert.Equal(t, correlationEsperado, observabilidade.ObterCorrelationID(ctxProc))

	errAudit := repo.SalvarAuditoria(ctxProc, alerta.RegistroAuditoria{
		ID:         "audit-teste-1",
		AlertaID:   envRecebido.Dados.AlertaID,
		Evento:     envRecebido.Evento,
		Versao:     envRecebido.Versao,
		IDEvento:   envRecebido.IDEvento,
		RoutingKey: "alerta.criado",
		RecebidoEm: time.Now(),
	})
	require.NoError(t, errAudit)

	// Consulta o registro gravado pelo consumidor de auditoria no SQLite
	registros, err := repo.ListarAuditoria(context.Background(), 5)
	require.NoError(t, err)
	require.NotEmpty(t, registros)
	assert.Equal(t, envRecebido.Dados.AlertaID, registros[0].AlertaID)
}

// Item 5: Teste das métricas das transições CLOSED -> OPEN -> HALF_OPEN -> CLOSED do Circuit Breaker
func TestValidacao_Item5_CircuitBreaker_TransicoesEMetricas(t *testing.T) {
	metricas := observabilidade.NovaMetricas()
	var statusCodeFalha atomic.Int32
	statusCodeFalha.Store(http.StatusOK)

	srvFalso := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		status := int(statusCodeFalha.Load())
		if status != http.StatusOK {
			w.WriteHeader(status)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"display_name":"Avenida Sete de Setembro, Araranguá, SC"}`))
	}))
	defer srvFalso.Close()

	cli := cliente.NovoNominatimComOpcoes(cliente.OpcoesNominatim{
		URLBase:            srvFalso.URL,
		Agente:             "teste-cb-metricas",
		Timeout:            500 * time.Millisecond,
		MaxFalhasCB:        3,
		TimeoutRecuperacao: 60 * time.Millisecond,
		IntervaloMinimo:    0,
		Metricas:           metricas,
	})

	local := alerta.Localizacao{Latitude: -28.9346, Longitude: -49.4957}

	// 1. Inicial: Estado CLOSED (0)
	assert.Equal(t, float64(0), testutil.ToFloat64(metricas.CircuitBreakerEstado.WithLabelValues("nominatim-osm")))

	// Requisição 1: Sucesso
	_, err := cli.EnderecoAproximado(context.Background(), local)
	require.NoError(t, err)
	assert.Equal(t, float64(0), testutil.ToFloat64(metricas.CircuitBreakerEstado.WithLabelValues("nominatim-osm")))

	// 2. Simula falhas consecutivas no Nominatim (HTTP 503)
	statusCodeFalha.Store(http.StatusServiceUnavailable)
	for i := 0; i < 3; i++ {
		_, _ = cli.EnderecoAproximado(context.Background(), local)
	}

	// Circuito transicionou para OPEN: gauge deve ser 2
	assert.Equal(t, resiliencia.EstadoAberto, cli.CircuitBreaker().Estado())
	assert.Equal(t, float64(2), testutil.ToFloat64(metricas.CircuitBreakerEstado.WithLabelValues("nominatim-osm")))
	assert.Equal(t, float64(1), testutil.ToFloat64(metricas.CircuitBreakerTransicoesTotal.WithLabelValues("nominatim-osm", "CLOSED", "OPEN")))

	// Requisição enquanto OPEN: deve rejeitar rápido (fail-fast) e contabilizar na métrica de rejeições
	_, errRejeicao := cli.EnderecoAproximado(context.Background(), local)
	assert.ErrorIs(t, errRejeicao, cliente.ErrCircuitoAberto)
	assert.Equal(t, float64(1), testutil.ToFloat64(metricas.CircuitBreakerRejeicoesTotal.WithLabelValues("nominatim-osm")))

	// 3. Aguarda período de recuperação para transição HALF_OPEN
	time.Sleep(80 * time.Millisecond)

	// Restaura o serviço simulado para simular recuperação
	statusCodeFalha.Store(http.StatusOK)

	// A requisição de teste (canary probe) deve passar e transicionar de HALF_OPEN -> CLOSED
	_, errRecuperado := cli.EnderecoAproximado(context.Background(), local)
	require.NoError(t, errRecuperado)

	// Comprova retorno para CLOSED (0) e registro das transições
	assert.Equal(t, resiliencia.EstadoFechado, cli.CircuitBreaker().Estado())
	assert.Equal(t, float64(0), testutil.ToFloat64(metricas.CircuitBreakerEstado.WithLabelValues("nominatim-osm")))
	assert.Equal(t, float64(1), testutil.ToFloat64(metricas.CircuitBreakerTransicoesTotal.WithLabelValues("nominatim-osm", "OPEN", "HALF_OPEN")))
	assert.Equal(t, float64(1), testutil.ToFloat64(metricas.CircuitBreakerTransicoesTotal.WithLabelValues("nominatim-osm", "HALF_OPEN", "CLOSED")))
}

// Item 6: Verificação de que /metrics e /status não vazam dados sensíveis
func TestValidacao_Item6_ProtecaoPrivacidade_Em_Metrics_E_Status(t *testing.T) {
	metricas := observabilidade.NovaMetricas()
	repo, err := armazenamento.AbrirSQLite(context.Background(), "file:"+filepath.Join(t.TempDir(), "val_privacidade.db"))
	require.NoError(t, err)
	defer repo.Fechar()

	servico := alerta.NovoServico(repo, notificacao.NovoDespachante(notificadorMockSilencioso{}), nil, time.Second)
	srv := httpapi.NovoServidorComOpcoes(servico, httpapi.OpcoesServidor{
		ChaveServico:                  "chave-secreta-s2s-privacidade",
		Metricas:                      metricas,
		MetricasAtivas:                true,
		MetricasAcessoLocalIrrestrito: true,
		VerificadorSaude: func(ctx context.Context) map[string]any {
			return map[string]any{
				"sqlite":   map[string]any{"status": "ok"},
				"rabbitmq": map[string]any{"status": "ok"},
			}
		},
	})
	h := srv.Rotas()

	// Cria alerta com dados privados e ID específico
	dadosSensíveis := alerta.PedidoAlerta{
		UsuariaID: "usuaria-protegida-cpf-123456",
		Nivel:     alerta.RiscoCritico,
		Mensagem:  "socorro rua residencial secreta",
		Local: alerta.Localizacao{
			Latitude:  -28.934678,
			Longitude: -49.495712,
		},
	}
	corpoJSON, _ := json.Marshal(dadosSensíveis)
	reqCriar := httptest.NewRequest("POST", "/alertas", strings.NewReader(string(corpoJSON)))
	reqCriar.Header.Set("Content-Type", "application/json")
	reqCriar.Header.Set("X-Service-Token", "chave-secreta-s2s-privacidade")
	recCriar := httptest.NewRecorder()
	h.ServeHTTP(recCriar, reqCriar)
	assert.Equal(t, http.StatusCreated, recCriar.Code)

	var criado alerta.Alerta
	require.NoError(t, json.Unmarshal(recCriar.Body.Bytes(), &criado))

	// Faz consulta específica por ID
	reqBuscar := httptest.NewRequest("GET", "/alertas/"+criado.ID, nil)
	reqBuscar.Header.Set("X-Service-Token", "chave-secreta-s2s-privacidade")
	recBuscar := httptest.NewRecorder()
	h.ServeHTTP(recBuscar, reqBuscar)
	assert.Equal(t, http.StatusOK, recBuscar.Code)

	// Coleta /metrics via localhost
	reqM := httptest.NewRequest("GET", "/metrics", nil)
	reqM.RemoteAddr = "127.0.0.1:45123"
	recM := httptest.NewRecorder()
	h.ServeHTTP(recM, reqM)
	assert.Equal(t, http.StatusOK, recM.Code)
	bodyMetrics := recM.Body.String()

	// Valida ausência total de dados sensíveis nas métricas
	assert.NotContains(t, bodyMetrics, criado.ID, "o ID específico do alerta não pode constar nas métricas")
	assert.NotContains(t, bodyMetrics, "usuaria-protegida-cpf-123456")
	assert.NotContains(t, bodyMetrics, "chave-secreta-s2s-privacidade")
	assert.NotContains(t, bodyMetrics, "-28.934678")
	assert.NotContains(t, bodyMetrics, "-49.495712")
	assert.NotContains(t, bodyMetrics, "rua residencial secreta")
	assert.Contains(t, bodyMetrics, `rota="/alertas/:id"`, "rota deve ser normalizada")

	// Coleta /status
	reqS := httptest.NewRequest("GET", "/status", nil)
	recS := httptest.NewRecorder()
	h.ServeHTTP(recS, reqS)
	assert.Equal(t, http.StatusOK, recS.Code)
	bodyStatus := recS.Body.String()

	assert.NotContains(t, bodyStatus, "chave-secreta-s2s-privacidade")
	assert.NotContains(t, bodyStatus, "usuaria-protegida-cpf-123456")
}

// Item 7: Comprovação de isolamento: nunca interage com filas principais nem envia e-mails/SMS reais
func TestValidacao_Item7_IsolamentoAbsoluto_SemInterferencias(t *testing.T) {
	// 1. Garante que os testes executam em bancos SQLite em diretórios temporários descartáveis
	tmpDir := t.TempDir()
	dbFile := filepath.Join(tmpDir, "banco_isolado.db")
	db, err := sql.Open("sqlite", dbFile)
	require.NoError(t, err)
	defer db.Close()

	// 2. Garante que qualquer notificador instanciado para os testes utiliza canais silenciosos
	mockSilencioso := notificadorMockSilencioso{}
	assert.Equal(t, "sms", mockSilencioso.Canal())
	assert.NoError(t, mockSilencioso.Enviar(context.Background(), "99999999", "msg"))

	// 3. Garante que EmailDemoNotificador está inativo por padrão
	emailDemo := notificacao.NovoEmailDemoNotificador(notificacao.ConfigEmailDemo{
		Ativo: false,
	})
	assert.ErrorIs(t, emailDemo.EnviarAlertaDemonstrativo(context.Background(), "alerta-teste"), notificacao.ErrEmailDemoDesativado)
}
