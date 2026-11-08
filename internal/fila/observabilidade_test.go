package fila_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/plataforma-mulher/botao-panico/internal/alerta"
	"github.com/plataforma-mulher/botao-panico/internal/armazenamento"
	"github.com/plataforma-mulher/botao-panico/internal/fila"
	"github.com/plataforma-mulher/botao-panico/internal/observabilidade"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFila_CorrelationID_E2E_OutboxRelay(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "observabilidade_outbox.db")
	repo, err := armazenamento.AbrirSQLite(context.Background(), "file:"+dbPath)
	require.NoError(t, err)
	defer repo.Fechar()

	metricas := observabilidade.NovaMetricas()
	despachante := &despachanteMock{}
	servico := alerta.NovoServico(repo, despachante, nil, 5*time.Second)

	pub := &publicadorMock{}
	relay := fila.NovoOutboxRelay(repo, pub, "alerta.criado", 50*time.Millisecond)
	relay.ConectarMetricas(metricas)

	servico.AtivarModoAssincrono(relay.NotificarNovoAlerta)
	relay.Iniciar()
	defer relay.Parar()

	// 1. Cria alerta com Correlation ID injetado no Context
	traceEsperado := "trace-acad-cit7219-456"
	ctxComTrace := observabilidade.InjetarCorrelationID(context.Background(), traceEsperado)

	pedido := alerta.PedidoAlerta{
		UsuariaID: "usuaria-observabilidade",
		Nivel:     alerta.RiscoAlto,
		Mensagem:  "teste de observabilidade e correlation id",
		Local: alerta.Localizacao{
			Latitude:  -28.9346,
			Longitude: -49.4957,
		},
	}

	alertaCriado, err := servico.Acionar(ctxComTrace, pedido)
	require.NoError(t, err)
	assert.NotEmpty(t, alertaCriado.ID)

	// Aguarda processamento pelo Outbox Relay
	require.Eventually(t, func() bool {
		pub.mu.Lock()
		defer pub.mu.Unlock()
		return len(pub.publicados) >= 1
	}, 2*time.Second, 50*time.Millisecond)

	pub.mu.Lock()
	envPublicado := pub.publicados[0]
	pub.mu.Unlock()

	// 2. Comprova propagação do Correlation ID
	assert.Equal(t, traceEsperado, envPublicado.CorrelationID, "o Correlation ID deve ser preservado do contexto até o envelope publicado")

	// 3. Comprova atualização de métricas do Outbox e RabbitMQ
	// OutboxTarefasPendentes deve estar em 0 após o envio bem-sucedido
	assert.Equal(t, float64(0), testutil.ToFloat64(metricas.OutboxTarefasPendentes))
	assert.Equal(t, float64(1), testutil.ToFloat64(metricas.RabbitMQPublicadasTotal.WithLabelValues("panico.eventos", "alerta.criado", "ack")))
}
