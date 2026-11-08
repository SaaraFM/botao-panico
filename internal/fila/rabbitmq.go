// Package fila implementa a infraestrutura de mensageria AMQP 0-9-1 com RabbitMQ,
// atendendo aos requisitos de Pub/Sub com múltiplos consumidores, Dead-Letter Queue (DLQ),
// confirmações manuais (ACK/NACK), QoS e persistência atômica via Transactional Outbox.
package fila

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/plataforma-mulher/botao-panico/internal/alerta"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Erros comuns de mensageria.
var (
	ErrBrokerIndisponivel = errors.New("conexão com o broker rabbitmq indisponível")
	ErrPublicacaoNack     = errors.New("publicação rejeitada pelo broker rabbitmq (nack)")
)

// OpcoesRabbitMQ reúne os parâmetros de configuração da infraestrutura de mensagens.
type OpcoesRabbitMQ struct {
	URL                    string
	ExchangeEventos        string
	ExchangeDLX            string
	FilaNotificacoes       string
	RoutingKeyNotificacoes string
	FilaAuditoria          string
	RoutingKeyAuditoria    string
	FilaDLQ                string
}

// GerenciadorRabbitMQ gerencia a conexão, topologia (exchanges, filas, bindings) e publicação.
type GerenciadorRabbitMQ struct {
	opts       OpcoesRabbitMQ
	conexao    *amqp.Connection
	canalPub   *amqp.Channel
	confirmaCh chan amqp.Confirmation
	mu         sync.Mutex
	fechado    bool
}

// NovoGerenciadorRabbitMQ estabelece conexão e declara a topologia completa de mensageria.
func NovoGerenciadorRabbitMQ(opts OpcoesRabbitMQ) (*GerenciadorRabbitMQ, error) {
	if opts.ExchangeEventos == "" {
		opts.ExchangeEventos = "panico.eventos"
	}
	if opts.ExchangeDLX == "" {
		opts.ExchangeDLX = "panico.eventos.dlx"
	}
	if opts.FilaNotificacoes == "" {
		opts.FilaNotificacoes = "panico.alertas.notificacoes"
	}
	if opts.RoutingKeyNotificacoes == "" {
		opts.RoutingKeyNotificacoes = "alerta.criado"
	}
	if opts.FilaAuditoria == "" {
		opts.FilaAuditoria = "panico.alertas.auditoria"
	}
	if opts.RoutingKeyAuditoria == "" {
		opts.RoutingKeyAuditoria = "#"
	}
	if opts.FilaDLQ == "" {
		opts.FilaDLQ = "panico.alertas.dlq"
	}

	g := &GerenciadorRabbitMQ{opts: opts}
	if err := g.conectarEDeclarar(); err != nil {
		return nil, err
	}
	return g, nil
}

// ConectarComRetentativas tenta conectar ao RabbitMQ por um período limite antes de falhar.
func ConectarComRetentativas(ctx context.Context, opts OpcoesRabbitMQ, tentativas int, intervalo time.Duration) (*GerenciadorRabbitMQ, error) {
	var ultimoErro error
	for i := 1; i <= tentativas; i++ {
		g, err := NovoGerenciadorRabbitMQ(opts)
		if err == nil {
			return g, nil
		}
		ultimoErro = err
		slog.Warn("rabbitmq indisponível, aguardando reconexão...",
			"tentativa", i,
			"max_tentativas", tentativas,
			"erro", err.Error(),
		)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(intervalo):
		}
	}
	return nil, fmt.Errorf("falha ao conectar no rabbitmq após %d tentativas: %w", tentativas, ultimoErro)
}

func (g *GerenciadorRabbitMQ) conectarEDeclarar() error {
	conn, err := amqp.Dial(g.opts.URL)
	if err != nil {
		return fmt.Errorf("conectar ao rabbitmq: %w", err)
	}

	canal, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("abrir canal de configuração: %w", err)
	}
	defer canal.Close()

	// 1. Exchange Principal (tipo topic, durável):
	// Uma exchange do tipo "topic" roteia mensagens para filas cujas chaves de ligação (binding keys)
	// casem com a routing key da mensagem. Se for "alerta.criado", vai para notificações;
	// se for "#" (curinga que casa com zero ou mais palavras), entrega uma cópia para auditoria.
	if err := canal.ExchangeDeclare(
		g.opts.ExchangeEventos,
		"topic",
		true,  // durable: sobrevive ao reinício do RabbitMQ
		false, // autoDelete: não é apagada quando os consumidores desconectam
		false, // internal
		false, // noWait
		nil,
	); err != nil {
		_ = conn.Close()
		return fmt.Errorf("declarar exchange topic %q: %w", g.opts.ExchangeEventos, err)
	}

	// 2. Exchange DLX (Dead-Letter Exchange, durável, tipo fanout):
	// Atua como central de descarte/quarentena. Mensagens rejeitadas pelos consumidores
	// sem requeue são encaminhadas automaticamente pelo RabbitMQ para esta exchange.
	if err := canal.ExchangeDeclare(
		g.opts.ExchangeDLX,
		"fanout",
		true,  // durable
		false, // autoDelete
		false, // internal
		false, // noWait
		nil,
	); err != nil {
		_ = conn.Close()
		return fmt.Errorf("declarar exchange dlx %q: %w", g.opts.ExchangeDLX, err)
	}

	// 3. Fila DLQ (Dead-Letter Queue, durável) ligada à DLX:
	// Armazena as mensagens com veneno (poison pills) para análise forense posterior,
	// impedindo travamento de workers e loops infinitos de reprocessamento.
	filaDLQ, err := canal.QueueDeclare(
		g.opts.FilaDLQ,
		true,  // durable
		false, // autoDelete
		false, // exclusive
		false, // noWait
		nil,
	)
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("declarar fila dlq %q: %w", g.opts.FilaDLQ, err)
	}
	if err := canal.QueueBind(filaDLQ.Name, "", g.opts.ExchangeDLX, false, nil); err != nil {
		_ = conn.Close()
		return fmt.Errorf("ligar fila dlq à dlx: %w", err)
	}

	// Propriedade x-dead-letter-exchange: instrui o RabbitMQ a desviar mensagens rejeitadas para a DLX
	argsDLX := amqp.Table{
		"x-dead-letter-exchange": g.opts.ExchangeDLX,
	}

	// 4. Fila Notificações (Consumidor A) ligada à exchange principal com chave exata
	filaNotif, err := canal.QueueDeclare(
		g.opts.FilaNotificacoes,
		true,  // durable
		false, // autoDelete
		false, // exclusive
		false, // noWait
		argsDLX,
	)
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("declarar fila notificacoes %q: %w", g.opts.FilaNotificacoes, err)
	}
	if err := canal.QueueBind(filaNotif.Name, g.opts.RoutingKeyNotificacoes, g.opts.ExchangeEventos, false, nil); err != nil {
		_ = conn.Close()
		return fmt.Errorf("ligar fila notificacoes à exchange: %w", err)
	}

	// 5. Fila Auditoria (Consumidor B) com binding '#' (recebe todos os eventos da exchange)
	filaAudit, err := canal.QueueDeclare(
		g.opts.FilaAuditoria,
		true,  // durable
		false, // autoDelete
		false, // exclusive
		false, // noWait
		argsDLX,
	)
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("declarar fila auditoria %q: %w", g.opts.FilaAuditoria, err)
	}
	if err := canal.QueueBind(filaAudit.Name, g.opts.RoutingKeyAuditoria, g.opts.ExchangeEventos, false, nil); err != nil {
		_ = conn.Close()
		return fmt.Errorf("ligar fila auditoria à exchange: %w", err)
	}

	// 6. Canal exclusivo para Publicação com Publisher Confirms habilitado:
	// O Publisher Confirms é o mecanismo em que o broker RabbitMQ envia uma confirmação assíncrona (ACK)
	// para o publicador assim que a mensagem for gravada com segurança no disco.
	canalPub, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("abrir canal de publicação: %w", err)
	}
	if err := canalPub.Confirm(false); err != nil {
		_ = canalPub.Close()
		_ = conn.Close()
		return fmt.Errorf("habilitar publisher confirms: %w", err)
	}

	g.conexao = conn
	g.canalPub = canalPub
	g.confirmaCh = canalPub.NotifyPublish(make(chan amqp.Confirmation, 10))

	slog.Info("topologia rabbitmq declarada com sucesso",
		"exchange", g.opts.ExchangeEventos,
		"dlx", g.opts.ExchangeDLX,
		"fila_notificacoes", g.opts.FilaNotificacoes,
		"fila_auditoria", g.opts.FilaAuditoria,
		"fila_dlq", g.opts.FilaDLQ,
	)
	return nil
}

// Conexao expõe a conexão AMQP subjacente para os consumidores abrirem seus próprios canais.
func (g *GerenciadorRabbitMQ) Conexao() *amqp.Connection {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.conexao
}

// Publicar envia um envelope para a exchange topic e aguarda a confirmação do broker (Publisher Confirms).
func (g *GerenciadorRabbitMQ) Publicar(ctx context.Context, routingKey string, env alerta.Envelope) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.fechado || g.canalPub == nil || g.conexao == nil || g.conexao.IsClosed() {
		return ErrBrokerIndisponivel
	}

	corpo, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("serializar envelope rabbitmq: %w", err)
	}

	err = g.canalPub.PublishWithContext(
		ctx,
		g.opts.ExchangeEventos,
		routingKey,
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType:     "application/json",
			ContentEncoding: "utf-8",
			DeliveryMode:    amqp.Persistent, // Mensagem gravada em disco no broker
			MessageId:       env.IDEvento,
			CorrelationId:   env.CorrelationID,
			Headers: amqp.Table{
				"correlation_id": env.CorrelationID,
			},
			Timestamp:       env.EmitidoEm,
			Type:            env.Evento,
			Body:            corpo,
		},
	)
	if err != nil {
		return fmt.Errorf("publicar mensagem amqp: %w", err)
	}

	// Aguarda publisher confirm
	select {
	case <-ctx.Done():
		return ctx.Err()
	case confirm, ok := <-g.confirmaCh:
		if !ok || !confirm.Ack {
			slog.Error("publicação rejeitada pelo broker (nack)",
				"evento", env.Evento,
				"id_evento", env.IDEvento,
				"routing_key", routingKey,
			)
			return ErrPublicacaoNack
		}
		slog.Info("publicação confirmada pelo broker (ack)",
			"evento", env.Evento,
			"id_evento", env.IDEvento,
			"routing_key", routingKey,
			"delivery_tag", confirm.DeliveryTag,
		)
		return nil
	}
}

// Fechar encerra o canal de publicação e a conexão com o RabbitMQ.
func (g *GerenciadorRabbitMQ) Fechar() error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.fechado {
		return nil
	}
	g.fechado = true

	var errs []error
	if g.canalPub != nil {
		if err := g.canalPub.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if g.conexao != nil && !g.conexao.IsClosed() {
		if err := g.conexao.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}
