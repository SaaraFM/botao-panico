package fila

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/plataforma-mulher/botao-panico/internal/alerta"
	"github.com/plataforma-mulher/botao-panico/internal/notificacao"
	"github.com/plataforma-mulher/botao-panico/internal/observabilidade"
	amqp "github.com/rabbitmq/amqp091-go"
)

// ConsumidorNotificacoes (Consumidor A) é o worker responsável por processar as notificações
// simuladas (e o e-mail de demonstração opcional) associadas ao alerta.
type ConsumidorNotificacoes struct {
	conn       *amqp.Connection
	canal      *amqp.Channel
	servico    *alerta.Servico
	repo       alerta.RepositorioOutbox
	emailDemo  *notificacao.EmailDemoNotificador
	metricas   *observabilidade.Metricas
	fila       string
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	mu         sync.Mutex
	executando bool
	tentativas map[string]int // alertaID -> contagem de tentativas locais
}

// OpcoesConsumidorNotificacoes reúne parâmetros do consumidor de notificações.
type OpcoesConsumidorNotificacoes struct {
	Fila      string
	EmailDemo *notificacao.EmailDemoNotificador
	Metricas  *observabilidade.Metricas
}

// NovoConsumidorNotificacoes instancia o Consumidor A.
func NovoConsumidorNotificacoes(
	conn *amqp.Connection,
	servico *alerta.Servico,
	repo alerta.RepositorioOutbox,
	opts OpcoesConsumidorNotificacoes,
) (*ConsumidorNotificacoes, error) {
	if conn == nil || conn.IsClosed() {
		return nil, ErrBrokerIndisponivel
	}
	if opts.Fila == "" {
		opts.Fila = "panico.alertas.notificacoes"
	}

	canal, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("abrir canal para consumidor de notificações: %w", err)
	}

	// QoS (Quality of Service) com prefetch count = 1:
	if err := canal.Qos(1, 0, false); err != nil {
		_ = canal.Close()
		return nil, fmt.Errorf("configurar qos: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &ConsumidorNotificacoes{
		conn:       conn,
		canal:      canal,
		servico:    servico,
		repo:       repo,
		emailDemo:  opts.EmailDemo,
		metricas:   opts.Metricas,
		fila:       opts.Fila,
		ctx:        ctx,
		cancel:     cancel,
		tentativas: make(map[string]int),
	}, nil
}

// Iniciar começa a consumir mensagens da fila de notificações.
func (c *ConsumidorNotificacoes) Iniciar() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.executando {
		return nil
	}

	entregas, err := c.canal.Consume(
		c.fila,
		"worker-notificacoes",
		false, // autoAck = false (ACK manual obrigatório)
		false, // exclusive
		false, // noLocal
		false, // noWait
		nil,
	)
	if err != nil {
		return fmt.Errorf("iniciar consumo na fila %q: %w", c.fila, err)
	}

	c.executando = true
	c.wg.Add(1)
	go c.loop(entregas)

	slog.Info("consumidor de notificações (Consumidor A) iniciado", "fila", c.fila)
	return nil
}

func (c *ConsumidorNotificacoes) loop(entregas <-chan amqp.Delivery) {
	defer c.wg.Done()

	for {
		select {
		case <-c.ctx.Done():
			return
		case entrega, ok := <-entregas:
			if !ok {
				slog.Warn("canal de entregas do rabbitmq fechado (notificações)")
				return
			}
			c.processarUmaEntrega(entrega)
		}
	}
}

func (c *ConsumidorNotificacoes) processarUmaEntrega(entrega amqp.Delivery) {
	inicio := time.Now()

	// 1. Decodificação do envelope padronizado (EVENTOS.md)
	var env alerta.Envelope
	if err := json.Unmarshal(entrega.Body, &env); err != nil {
		// Mensagem com payload inválido / poison pill:
		// NACK sem requeue para encaminhar imediatamente à Dead-Letter Queue (DLQ)!
		slog.Warn("mensagem com corpo inválido descartada sem requeue (encaminhada à DLQ)",
			"fila", c.fila,
			"delivery_tag", entrega.DeliveryTag,
			"erro", err.Error(),
		)
		if c.metricas != nil {
			c.metricas.RegistrarFalhaDLQ("notificacoes", "payload_invalido")
		}
		_ = entrega.Nack(false, false)
		return
	}

	alertaID := env.Dados.AlertaID
	if alertaID == "" {
		slog.Warn("envelope sem alerta_id válido, descartando para DLQ", "id_evento", env.IDEvento)
		if c.metricas != nil {
			c.metricas.RegistrarFalhaDLQ("notificacoes", "alerta_id_ausente")
		}
		_ = entrega.Nack(false, false)
		return
	}

	corrID := env.CorrelationID
	if corrID == "" {
		corrID = entrega.CorrelationId
	}
	corrID = observabilidade.ExtrairOuGerarCorrelationID(corrID)

	slog.Info("mensagem recebida pelo consumidor de notificações",
		"evento", env.Evento,
		"id_evento", env.IDEvento,
		"correlation_id", corrID,
		"alerta_id", alertaID,
		"delivery_tag", entrega.DeliveryTag,
	)

	// 2. Processa as notificações do alerta no SQLite com Correlation ID injetado no context
	ctxProc, cancelProc := context.WithTimeout(c.ctx, 10*time.Second)
	defer cancelProc()
	ctxProc = observabilidade.InjetarCorrelationID(ctxProc, corrID)

	err := c.servico.ProcessarTarefaNotificacao(ctxProc, alertaID)
	if err != nil {
		c.mu.Lock()
		c.tentativas[alertaID]++
		tent := c.tentativas[alertaID]
		c.mu.Unlock()

		if tent >= 3 {
			slog.Error("limite de retentativas excedido; encaminhando mensagem para a DLQ",
				"alerta_id", alertaID,
				"correlation_id", corrID,
				"tentativas", tent,
				"erro", err.Error(),
			)
			if c.metricas != nil {
				c.metricas.RegistrarFalhaDLQ("notificacoes", "limite_retentativas")
			}
			_ = entrega.Nack(false, false) // Sem requeue -> DLQ
			return
		}

		slog.Warn("erro ao processar notificações do alerta; devolvendo à fila (NACK com requeue)",
			"alerta_id", alertaID,
			"correlation_id", corrID,
			"tentativa", tent,
			"erro", err.Error(),
		)
		if c.metricas != nil {
			c.metricas.RegistrarConsumoRabbitMQ("notificacoes", "nack_requeue")
		}
		_ = entrega.Nack(false, true)
		return
	}

	// 3. E-mail demonstrativo real opcional (Modo B - apenas se explicitamente habilitado)
	if c.emailDemo != nil {
		if errEmail := c.emailDemo.EnviarAlertaDemonstrativo(ctxProc, alertaID); errEmail != nil {
			slog.Warn("falha ao enviar e-mail demonstrativo; alerta preservado",
				"alerta_id", alertaID,
				"correlation_id", corrID,
				"erro", errEmail.Error(),
			)
		}
	}

	// 4. Marca a tarefa da Outbox como concluída
	if env.Dados.TarefaID != "" {
		_ = c.repo.MarcarTarefaConcluida(ctxProc, env.Dados.TarefaID)
	}

	// 5. ACK (Acknowledgement) manual após gravação e sucesso no banco:
	if err := entrega.Ack(false); err != nil {
		slog.Error("falha ao enviar ACK no rabbitmq", "alerta_id", alertaID, "correlation_id", corrID, "erro", err)
		return
	}

	if c.metricas != nil {
		c.metricas.RegistrarConsumoRabbitMQ("notificacoes", "sucesso")
	}

	slog.Info("notificações processadas e confirmadas com sucesso (ACK manual)",
		"evento", env.Evento,
		"id_evento", env.IDEvento,
		"correlation_id", corrID,
		"alerta_id", alertaID,
		"duracao_ms", time.Since(inicio).Milliseconds(),
	)
}

// Parar interrompe o consumo ordenadamente.
func (c *ConsumidorNotificacoes) Parar(timeout time.Duration) {
	c.mu.Lock()
	if !c.executando {
		c.mu.Unlock()
		return
	}
	c.executando = false
	c.cancel()
	_ = c.canal.Close()
	c.mu.Unlock()

	terminou := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(terminou)
	}()

	select {
	case <-terminou:
		slog.Info("consumidor de notificações finalizado com sucesso")
	case <-time.After(timeout):
		slog.Warn("timeout ao aguardar finalização do consumidor de notificações")
	}
}
