package observabilidade

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// Buckets padrão de latência em segundos para operações web e de mensageria
	bucketsLatencia = []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5}
)

// Metricas centraliza os coletores Prometheus para monitoramento do microsserviço.
// Não utiliza labels com informações pessoais, IDs de usuárias ou coordenadas geográficas (LGPD).
type Metricas struct {
	reg *prometheus.Registry

	// 1. Requisições e Latência HTTP
	HTTPRequestsTotal          *prometheus.CounterVec
	HTTPRequestDurationSeconds *prometheus.HistogramVec

	// 2. Requisições e Latência gRPC
	GRPCRequestsTotal          *prometheus.CounterVec
	GRPCRequestDurationSeconds *prometheus.HistogramVec

	// 3. RabbitMQ e Transactional Outbox
	RabbitMQPublicadasTotal *prometheus.CounterVec
	RabbitMQConsumidasTotal *prometheus.CounterVec
	RabbitMQDLQTotal        *prometheus.CounterVec
	OutboxTarefasPendentes  prometheus.Gauge

	// 4. Circuit Breaker
	CircuitBreakerEstado          *prometheus.GaugeVec
	CircuitBreakerRejeicoesTotal  *prometheus.CounterVec
	CircuitBreakerTransicoesTotal *prometheus.CounterVec
}

// NovaMetricas instancia e registra os coletores em um Registry isolado.
func NovaMetricas() *Metricas {
	reg := prometheus.NewRegistry()

	m := &Metricas{
		reg: reg,

		// 1. HTTP
		HTTPRequestsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "botao_panico",
				Subsystem: "http",
				Name:      "requests_total",
				Help:      "Total de requisições HTTP processadas, particionadas por método, rota e status.",
			},
			[]string{"metodo", "rota", "status"},
		),
		HTTPRequestDurationSeconds: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "botao_panico",
				Subsystem: "http",
				Name:      "request_duration_seconds",
				Help:      "Histograma da duração das requisições HTTP em segundos.",
				Buckets:   bucketsLatencia,
			},
			[]string{"metodo", "rota"},
		),

		// 2. gRPC
		GRPCRequestsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "botao_panico",
				Subsystem: "grpc",
				Name:      "requests_total",
				Help:      "Total de chamadas gRPC recebidas, particionadas por método e código gRPC.",
			},
			[]string{"metodo", "codigo"},
		),
		GRPCRequestDurationSeconds: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "botao_panico",
				Subsystem: "grpc",
				Name:      "request_duration_seconds",
				Help:      "Histograma da duração das chamadas gRPC em segundos.",
				Buckets:   bucketsLatencia,
			},
			[]string{"metodo"},
		),

		// 3. RabbitMQ & Outbox
		RabbitMQPublicadasTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "botao_panico",
				Subsystem: "rabbitmq",
				Name:      "messages_published_total",
				Help:      "Total de mensagens enviadas ao RabbitMQ pelo Outbox Relay com status de confirmação.",
			},
			[]string{"exchange", "routing_key", "status"},
		),
		RabbitMQConsumidasTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "botao_panico",
				Subsystem: "rabbitmq",
				Name:      "messages_consumed_total",
				Help:      "Total de mensagens recebidas e processadas pelos consumidores.",
			},
			[]string{"consumidor", "status"},
		),
		RabbitMQDLQTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "botao_panico",
				Subsystem: "rabbitmq",
				Name:      "dlq_messages_total",
				Help:      "Contador de mensagens rejeitadas pela aplicação que foram direcionadas à DLQ (distinto do backlog real do broker).",
			},
			[]string{"consumidor", "motivo"},
		),
		OutboxTarefasPendentes: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: "botao_panico",
				Subsystem: "outbox",
				Name:      "tarefas_pendentes",
				Help:      "Quantidade de tarefas atualmente pendentes no Transactional Outbox SQLite.",
			},
		),

		// 4. Circuit Breaker
		CircuitBreakerEstado: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "botao_panico",
				Subsystem: "circuit_breaker",
				Name:      "estado",
				Help:      "Estado atual do Circuit Breaker: 0=CLOSED, 1=HALF_OPEN, 2=OPEN.",
			},
			[]string{"nome"},
		),
		CircuitBreakerRejeicoesTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "botao_panico",
				Subsystem: "circuit_breaker",
				Name:      "rejections_total",
				Help:      "Total de requisições rejeitadas em fail-fast pelo Circuit Breaker.",
			},
			[]string{"nome"},
		),
		CircuitBreakerTransicoesTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "botao_panico",
				Subsystem: "circuit_breaker",
				Name:      "transitions_total",
				Help:      "Total de transições de estado ocorridas no Circuit Breaker.",
			},
			[]string{"nome", "de", "para"},
		),
	}

	reg.MustRegister(
		m.HTTPRequestsTotal,
		m.HTTPRequestDurationSeconds,
		m.GRPCRequestsTotal,
		m.GRPCRequestDurationSeconds,
		m.RabbitMQPublicadasTotal,
		m.RabbitMQConsumidasTotal,
		m.RabbitMQDLQTotal,
		m.OutboxTarefasPendentes,
		m.CircuitBreakerEstado,
		m.CircuitBreakerRejeicoesTotal,
		m.CircuitBreakerTransicoesTotal,
	)

	return m
}

// Handler devolve o http.Handler do Prometheus para servir o endpoint /metrics.
func (m *Metricas) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	})
}

// RegistrarRequisicaoHTTP computa métricas de uma requisição HTTP de forma thread-safe.
func (m *Metricas) RegistrarRequisicaoHTTP(metodo, rota string, status int, duracaoSegundos float64) {
	if m == nil {
		return
	}
	rotaNormalizada := NormalizarRota(rota)
	statusStr := strconv.Itoa(status)

	m.HTTPRequestsTotal.WithLabelValues(metodo, rotaNormalizada, statusStr).Inc()
	m.HTTPRequestDurationSeconds.WithLabelValues(metodo, rotaNormalizada).Observe(duracaoSegundos)
}

// RegistrarChamadaGRPC computa métricas de uma chamada gRPC unária.
func (m *Metricas) RegistrarChamadaGRPC(metodo, codigo string, duracaoSegundos float64) {
	if m == nil {
		return
	}
	m.GRPCRequestsTotal.WithLabelValues(metodo, codigo).Inc()
	m.GRPCRequestDurationSeconds.WithLabelValues(metodo).Observe(duracaoSegundos)
}

// RegistrarPublicacaoRabbitMQ computa uma publicação de mensagem pelo Outbox Relay.
func (m *Metricas) RegistrarPublicacaoRabbitMQ(exchange, routingKey, status string) {
	if m == nil {
		return
	}
	m.RabbitMQPublicadasTotal.WithLabelValues(exchange, routingKey, status).Inc()
}

// RegistrarConsumoRabbitMQ computa o resultado de processamento por um worker consumidor.
func (m *Metricas) RegistrarConsumoRabbitMQ(consumidor, status string) {
	if m == nil {
		return
	}
	m.RabbitMQConsumidasTotal.WithLabelValues(consumidor, status).Inc()
}

// RegistrarFalhaDLQ registra uma rejeição que resultou em envio para a Dead-Letter Queue.
func (m *Metricas) RegistrarFalhaDLQ(consumidor, motivo string) {
	if m == nil {
		return
	}
	m.RabbitMQDLQTotal.WithLabelValues(consumidor, motivo).Inc()
}

// AtualizarEstadoCircuitBreaker atualiza o medidor de estado do Circuit Breaker:
// 0=CLOSED, 1=HALF_OPEN, 2=OPEN.
func (m *Metricas) AtualizarEstadoCircuitBreaker(nome, estado string) {
	if m == nil {
		return
	}
	var valor float64
	switch estado {
	case "CLOSED":
		valor = 0
	case "HALF_OPEN":
		valor = 1
	case "OPEN":
		valor = 2
	default:
		valor = -1
	}
	m.CircuitBreakerEstado.WithLabelValues(nome).Set(valor)
}

// RegistrarTransicaoCircuitBreaker registra a contagem de transição de estado.
func (m *Metricas) RegistrarTransicaoCircuitBreaker(nome, de, para string) {
	if m == nil {
		return
	}
	m.CircuitBreakerTransicoesTotal.WithLabelValues(nome, de, para).Inc()
	m.AtualizarEstadoCircuitBreaker(nome, para)
}

// RegistrarRejeicaoCircuitBreaker computa uma rejeição fail-fast pelo circuito.
func (m *Metricas) RegistrarRejeicaoCircuitBreaker(nome string) {
	if m == nil {
		return
	}
	m.CircuitBreakerRejeicoesTotal.WithLabelValues(nome).Inc()
}

// NormalizarRota mapeia rotas com parâmetros variáveis (como IDs) para padrões estáticos,
// evitando explosão de cardinalidade e vazamento de dados de alertas nas métricas.
func NormalizarRota(caminho string) string {
	limpo := strings.TrimRight(caminho, "/")
	if limpo == "" {
		return "/"
	}
	if strings.HasPrefix(limpo, "/alertas/") {
		return "/alertas/:id"
	}
	if strings.HasPrefix(limpo, "/auditoria/") {
		return "/auditoria/:id"
	}
	return limpo
}
