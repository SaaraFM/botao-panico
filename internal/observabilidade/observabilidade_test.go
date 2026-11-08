package observabilidade_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/plataforma-mulher/botao-panico/internal/observabilidade"
	"github.com/stretchr/testify/assert"
)

func TestCorrelationID_Valido(t *testing.T) {
	idValido := "meu-trace-id_123.456"
	extraido := observabilidade.ExtrairOuGerarCorrelationID(idValido)
	assert.Equal(t, idValido, extraido)
}

func TestCorrelationID_InvalidoGeraNovo(t *testing.T) {
	casosInvalidos := []string{
		"",
		" ",
		"trace com espacos",
		"trace<invalido>",
		"trace\ncom\nquebras",
		strings.Repeat("a", 129), // excede 128 chars
	}

	for _, c := range casosInvalidos {
		gerado := observabilidade.ExtrairOuGerarCorrelationID(c)
		assert.True(t, strings.HasPrefix(gerado, "trace-"))
		assert.LessOrEqual(t, len(gerado), 64)
	}
}

func TestCorrelationID_Contexto(t *testing.T) {
	id := "trace-contexto-1"
	ctx := observabilidade.InjetarCorrelationID(context.Background(), id)

	recuperado := observabilidade.ObterCorrelationID(ctx)
	assert.Equal(t, id, recuperado)

	// Sem contexto, gera novo seguro
	semCtx := observabilidade.ObterCorrelationID(nil)
	assert.True(t, strings.HasPrefix(semCtx, "trace-"))
}

func TestMetricas_RegistrosEColeta(t *testing.T) {
	m := observabilidade.NovaMetricas()

	// 1. Registros HTTP
	m.RegistrarRequisicaoHTTP("POST", "/alertas", 201, 0.045)
	m.RegistrarRequisicaoHTTP("GET", "/alertas/alerta-teste-123456", 200, 0.012) // deve normalizar rota

	// 2. Registros gRPC
	m.RegistrarChamadaGRPC("/alerta.v1.AlertaService/AcionarAlerta", "OK", 0.035)

	// 3. RabbitMQ & Outbox
	m.RegistrarPublicacaoRabbitMQ("panico.eventos", "alerta.criado", "ack")
	m.RegistrarConsumoRabbitMQ("notificacoes", "sucesso")
	m.RegistrarFalhaDLQ("notificacoes", "limite_retentativas")
	m.OutboxTarefasPendentes.Set(3)

	// 4. Circuit Breaker
	m.AtualizarEstadoCircuitBreaker("nominatim-osm", "OPEN")
	m.RegistrarTransicaoCircuitBreaker("nominatim-osm", "CLOSED", "OPEN")
	m.RegistrarRejeicaoCircuitBreaker("nominatim-osm")

	// 5. Coleta via HTTP Handler do Prometheus
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/metrics", nil)
	m.Handler().ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	corpo := rec.Body.String()

	// Verifica presença das métricas geradas
	assert.Contains(t, corpo, "botao_panico_http_requests_total")
	assert.Contains(t, corpo, `rota="/alertas/:id"`, "a rota com ID dinâmico deve ser normalizada")
	assert.NotContains(t, corpo, "alerta-teste-123456", "o ID dinâmico não pode vazar como label")
	assert.Contains(t, corpo, "botao_panico_grpc_requests_total")
	assert.Contains(t, corpo, "botao_panico_rabbitmq_messages_published_total")
	assert.Contains(t, corpo, "botao_panico_rabbitmq_messages_consumed_total")
	assert.Contains(t, corpo, "botao_panico_rabbitmq_dlq_messages_total")
	assert.Contains(t, corpo, "botao_panico_outbox_tarefas_pendentes 3")
	assert.Contains(t, corpo, `botao_panico_circuit_breaker_estado{nome="nominatim-osm"} 2`)
	assert.Contains(t, corpo, "botao_panico_circuit_breaker_rejections_total")
}

func TestNormalizarRota(t *testing.T) {
	assert.Equal(t, "/alertas/:id", observabilidade.NormalizarRota("/alertas/123"))
	assert.Equal(t, "/alertas/:id", observabilidade.NormalizarRota("/alertas/alerta-abc-xyz/"))
	assert.Equal(t, "/alertas", observabilidade.NormalizarRota("/alertas"))
	assert.Equal(t, "/auditoria/:id", observabilidade.NormalizarRota("/auditoria/reg-1"))
	assert.Equal(t, "/", observabilidade.NormalizarRota(""))
}
