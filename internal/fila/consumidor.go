package fila

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/plataforma-mulher/botao-panico/internal/alerta"
)

// ConsumidorNotificacoes é o worker assíncrono que retira mensagens do NATS JetStream,
// recupera os dados no SQLite e despacha as notificações simuladas de forma idempotente.
type ConsumidorNotificacoes struct {
	js          nats.JetStreamContext
	servico     *alerta.Servico
	repo        alerta.RepositorioOutbox
	stream      string
	subject     string
	consumer    string
	grupoFila   string
	sub         *nats.Subscription
	travas      sync.Map // alerta_id -> *sync.Mutex (protege contra concorrência da mesma tarefa)
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	mu          sync.Mutex
	executando  bool
	numWorkers  int
}

// OpcoesConsumidor reúne configurações do worker de notificações.
type OpcoesConsumidor struct {
	Stream     string
	Subject    string
	Consumer   string
	GrupoFila  string
	NumWorkers int
}

// NovoConsumidorNotificacoes inicializa o consumidor durável do JetStream.
func NovoConsumidorNotificacoes(
	js nats.JetStreamContext,
	servico *alerta.Servico,
	repo alerta.RepositorioOutbox,
	opts OpcoesConsumidor,
) *ConsumidorNotificacoes {
	if opts.Stream == "" {
		opts.Stream = "ALERTAS"
	}
	if opts.Subject == "" {
		opts.Subject = "alertas.notificar"
	}
	if opts.Consumer == "" {
		opts.Consumer = "worker-notificacoes"
	}
	if opts.GrupoFila == "" {
		opts.GrupoFila = "workers-notificacoes"
	}
	if opts.NumWorkers <= 0 {
		opts.NumWorkers = 3
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &ConsumidorNotificacoes{
		js:         js,
		servico:    servico,
		repo:       repo,
		stream:     opts.Stream,
		subject:    opts.Subject,
		consumer:   opts.Consumer,
		grupoFila:  opts.GrupoFila,
		numWorkers: opts.NumWorkers,
		ctx:        ctx,
		cancel:     cancel,
	}
}

// Iniciar assina o NATS JetStream em modo Queue Subscription durável e inicia o pool de workers.
func (c *ConsumidorNotificacoes) Iniciar() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.executando {
		return nil
	}

	// QueueSubscribeSync durável: equilibra as tarefas entre réplicas e garante entrega persistente
	sub, err := c.js.QueueSubscribeSync(
		c.subject,
		c.grupoFila,
		nats.Durable(c.consumer),
		nats.DeliverAll(),
		nats.ManualAck(),
		nats.AckWait(10*time.Second),
		nats.MaxDeliver(5),
	)
	if err != nil {
		return err
	}
	c.sub = sub
	c.executando = true

	// Inicia pool de workers para consumo concorrente
	for i := 0; i < c.numWorkers; i++ {
		c.wg.Add(1)
		go c.workerLoop(i)
	}

	slog.Info("Worker de notificações NATS JetStream iniciado",
		"stream", c.stream, "subject", c.subject, "consumer", c.consumer, "pool", c.numWorkers)
	return nil
}

// workerLoop busca e processa mensagens individualmente com confirmação explícita (Ack).
func (c *ConsumidorNotificacoes) workerLoop(workerID int) {
	defer c.wg.Done()

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		// Busca a próxima mensagem respeitando o contexto
		msg, err := c.sub.NextMsg(500 * time.Millisecond)
		if err != nil {
			if errors.Is(err, nats.ErrTimeout) || errors.Is(err, nats.ErrBadSubscription) {
				continue
			}
			if errors.Is(err, nats.ErrConnectionClosed) || c.ctx.Err() != nil {
				return
			}
			continue
		}

		c.processarMensagem(msg)
	}
}

// processarMensagem executa a tarefa com garantia de idempotência e confirmação após o commit no banco.
func (c *ConsumidorNotificacoes) processarMensagem(msg *nats.Msg) {
	var evento alerta.EventoAlertaCriado
	if err := json.Unmarshal(msg.Data, &evento); err != nil {
		slog.Error("worker: mensagem corrompida na fila, descartando com Term", "erro", err)
		_ = msg.Term() // Descarta permanentemente mensagens não decodificáveis
		return
	}

	// 1. Controle de concorrência: bloqueio por alerta_id para impedir que dois workers processem a mesma tarefa simultaneamente
	val, _ := c.travas.LoadOrStore(evento.AlertaID, &sync.Mutex{})
	muAlerta := val.(*sync.Mutex)
	muAlerta.Lock()
	defer muAlerta.Unlock()

	ctxProcessamento, cancel := context.WithTimeout(c.ctx, 15*time.Second)
	defer cancel()

	// 2. Executa notificações simuladas de forma idempotente na camada de domínio
	err := c.servico.ProcessarTarefaNotificacao(ctxProcessamento, evento.AlertaID)
	if err != nil {
		slog.Warn("worker: falha ao processar tarefa de notificacao, agendando retry",
			"alerta_id", evento.AlertaID, "tarefa_id", evento.TarefaID, "erro", err)
		// Envia NAK com backoff para reentrega pelo JetStream
		_ = msg.NakWithDelay(2 * time.Second)
		return
	}

	// 3. Atualiza estado da tarefa Outbox para 'concluido' no SQLite
	if evento.TarefaID != "" {
		_ = c.repo.MarcarTarefaConcluida(context.Background(), evento.TarefaID)
	}

	// 4. Confirmação explícita (Ack) enviada ao NATS JetStream SOMENTE após persistência no SQLite
	if err := msg.Ack(); err != nil {
		slog.Error("worker: falha ao enviar ack ao nats", "alerta_id", evento.AlertaID, "erro", err)
	}
}

// Parar desliga o consumidor de forma graciosa, aguardando que tarefas em andamento terminem.
func (c *ConsumidorNotificacoes) Parar(timeout time.Duration) {
	c.mu.Lock()
	if !c.executando {
		c.mu.Unlock()
		return
	}
	c.executando = false
	c.mu.Unlock()

	// Drena a inscrição para que não receba novas mensagens e termine as atuais
	if c.sub != nil {
		_ = c.sub.Drain()
	}

	c.cancel()

	// Aguarda conclusão com prazo configurável
	canalFeito := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(canalFeito)
	}()

	select {
	case <-canalFeito:
		slog.Info("Worker de notificações finalizado graciosamente")
	case <-time.After(timeout):
		slog.Warn("Worker de notificações forçado a encerrar por timeout", "timeout", timeout)
	}
}
