package fila

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/plataforma-mulher/botao-panico/internal/alerta"
	amqp "github.com/rabbitmq/amqp091-go"
)

// ConsumidorAuditoria (Consumidor B) é o consumidor independente inspirado em auditoria-service.
// Utiliza a fila 'panico.alertas.auditoria' com binding '#' para auditar e arquivar de forma independente
// todos os eventos que trafegam pela exchange topic, comprovando o padrão Publish/Subscribe.
type ConsumidorAuditoria struct {
	conn       *amqp.Connection
	canal      *amqp.Channel
	servico    *alerta.Servico
	fila       string
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	mu         sync.Mutex
	executando bool
}

// OpcoesConsumidorAuditoria reúne configurações do Consumidor B.
type OpcoesConsumidorAuditoria struct {
	Fila string
}

// NovoConsumidorAuditoria instancia o Consumidor B de auditoria.
func NovoConsumidorAuditoria(
	conn *amqp.Connection,
	servico *alerta.Servico,
	opts OpcoesConsumidorAuditoria,
) (*ConsumidorAuditoria, error) {
	if conn == nil || conn.IsClosed() {
		return nil, ErrBrokerIndisponivel
	}
	if opts.Fila == "" {
		opts.Fila = "panico.alertas.auditoria"
	}

	canal, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("abrir canal para consumidor de auditoria: %w", err)
	}

	if err := canal.Qos(10, 0, false); err != nil {
		_ = canal.Close()
		return nil, fmt.Errorf("configurar qos de auditoria: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &ConsumidorAuditoria{
		conn:    conn,
		canal:   canal,
		servico: servico,
		fila:    opts.Fila,
		ctx:     ctx,
		cancel:  cancel,
	}, nil
}

// Iniciar começa a consumir eventos para registro de auditoria.
func (c *ConsumidorAuditoria) Iniciar() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.executando {
		return nil
	}

	entregas, err := c.canal.Consume(
		c.fila,
		"worker-auditoria",
		false, // ACK manual independente
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("iniciar consumo na fila de auditoria %q: %w", c.fila, err)
	}

	c.executando = true
	c.wg.Add(1)
	go c.loop(entregas)

	slog.Info("consumidor de auditoria (Consumidor B) iniciado", "fila", c.fila)
	return nil
}

func (c *ConsumidorAuditoria) loop(entregas <-chan amqp.Delivery) {
	defer c.wg.Done()

	for {
		select {
		case <-c.ctx.Done():
			return
		case entrega, ok := <-entregas:
			if !ok {
				slog.Warn("canal de entregas do rabbitmq fechado (auditoria)")
				return
			}
			c.processarUmaEntrega(entrega)
		}
	}
}

func (c *ConsumidorAuditoria) processarUmaEntrega(entrega amqp.Delivery) {
	// Padrão Publish/Subscribe: O Consumidor B recebe uma cópia independente do mesmo evento
	// publicado na exchange topic, sem interferir nem competir com o Consumidor A (Notificações).
	var env alerta.Envelope
	if err := json.Unmarshal(entrega.Body, &env); err != nil {
		slog.Warn("mensagem inválida recebida na auditoria; encaminhando para a DLQ",
			"fila", c.fila,
			"delivery_tag", entrega.DeliveryTag,
			"erro", err.Error(),
		)
		_ = entrega.Nack(false, false) // NACK sem requeue: RabbitMQ desvia para panico.eventos.dlx
		return
	}

	agora := time.Now().UTC()

	// 1. Log estruturado com slog contendo os metadados do evento recebido via Pub/Sub
	slog.Info("evento_auditado",
		"componente", "consumidor_auditoria",
		"fila", c.fila,
		"routing_key", entrega.RoutingKey,
		"evento", env.Evento,
		"versao", env.Versao,
		"id_evento", env.IDEvento,
		"alerta_id", env.Dados.AlertaID,
		"usuaria_id", env.Dados.UsuariaID,
		"emitido_em", env.EmitidoEm.Format(time.RFC3339),
		"recebido_em", agora.Format(time.RFC3339),
	)

	// 2. Persistência no SQLite na tabela de auditoria (se serviço disponível)
	if c.servico != nil {
		regID := novoIDAuditoria()
		_ = c.servico.SalvarAuditoria(c.ctx, alerta.RegistroAuditoria{
			ID:         regID,
			Evento:     env.Evento,
			Versao:     env.Versao,
			IDEvento:   env.IDEvento,
			AlertaID:   env.Dados.AlertaID,
			RoutingKey: entrega.RoutingKey,
			RecebidoEm: agora,
		})
	}

	// 3. Confirmação independente da mensagem
	if err := entrega.Ack(false); err != nil {
		slog.Error("falha ao enviar ACK na auditoria", "id_evento", env.IDEvento, "erro", err)
		return
	}
}

// Parar encerra o consumidor de auditoria graciosamente.
func (c *ConsumidorAuditoria) Parar(timeout time.Duration) {
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
		slog.Info("consumidor de auditoria finalizado com sucesso")
	case <-time.After(timeout):
		slog.Warn("timeout ao aguardar finalização do consumidor de auditoria")
	}
}

func novoIDAuditoria() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return "aud-" + hex.EncodeToString(b)
}
