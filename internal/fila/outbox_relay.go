package fila

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/plataforma-mulher/botao-panico/internal/alerta"
)

// Publicador define o contrato para publicação de eventos no broker de mensageria.
type Publicador interface {
	Publicar(ctx context.Context, routingKey string, env alerta.Envelope) error
}

// OutboxRelay ("Retransmissor do Outbox") é o componente de segundo plano que implementa
// a segunda metade do padrão Transactional Outbox:
// 1. O serviço salva o alerta e a tarefa no SQLite na mesma transação atômica.
// 2. O OutboxRelay lê continuamente tarefas com estado 'pendente' e publica no RabbitMQ.
// 3. Ao receber o Publisher Confirm do broker, atualiza o estado para 'publicado'.
// Isso garante o modelo de entrega "at-least-once": se o broker cair, as mensagens continuam
// salvas no SQLite aguardando reconexão, eliminando qualquer risco de perda de alertas.
type OutboxRelay struct {
	repo       alerta.RepositorioOutbox
	publicador Publicador
	topico     string
	intervalo  time.Duration
	gatilhoCh  chan struct{}
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	mu         sync.Mutex
	executando bool
}

// NovoOutboxRelay inicializa o publisher do Transactional Outbox.
func NovoOutboxRelay(
	repo alerta.RepositorioOutbox,
	pub Publicador,
	topicoPadrao string,
	intervaloVarredura ...time.Duration,
) *OutboxRelay {
	intervalo := 250 * time.Millisecond
	if len(intervaloVarredura) > 0 && intervaloVarredura[0] > 0 {
		intervalo = intervaloVarredura[0]
	}

	if topicoPadrao == "" {
		topicoPadrao = "alerta.criado"
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &OutboxRelay{
		repo:       repo,
		publicador: pub,
		topico:     topicoPadrao,
		intervalo:  intervalo,
		gatilhoCh:  make(chan struct{}, 1),
		ctx:        ctx,
		cancel:     cancel,
	}
}

// Iniciar dispara o loop de publicação em segundo plano.
func (r *OutboxRelay) Iniciar() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.executando {
		return
	}
	r.executando = true

	r.wg.Add(1)
	go r.loop()
}

// NotificarNovoAlerta avisa o relay para processar imediatamente sem esperar o próximo ticker.
func (r *OutboxRelay) NotificarNovoAlerta() {
	select {
	case r.gatilhoCh <- struct{}{}:
	default:
		// Se o canal já possui sinal pendente, nada a fazer
	}
}

// loop executa a varredura periódica e atende gatilhos imediatos.
func (r *OutboxRelay) loop() {
	defer r.wg.Done()

	ticker := time.NewTicker(r.intervalo)
	defer ticker.Stop()

	// Processa tarefas pendentes remanescentes de execuções anteriores (recuperação pós-reinicialização)
	r.processarLote(r.ctx)

	for {
		select {
		case <-r.ctx.Done():
			return
		case <-r.gatilhoCh:
			r.processarLote(r.ctx)
		case <-ticker.C:
			r.processarLote(r.ctx)
		}
	}
}

// processarLote busca até 20 tarefas pendentes e publica cada uma no RabbitMQ.
func (r *OutboxRelay) processarLote(ctx context.Context) {
	if r.publicador == nil {
		return
	}

	tarefas, err := r.repo.BuscarTarefasPendentes(ctx, 20)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			slog.Error("falha ao buscar tarefas pendentes na outbox", "erro", err)
		}
		return
	}

	for _, tarefa := range tarefas {
		if ctx.Err() != nil {
			return
		}
		r.processarUmaTarefa(ctx, tarefa)
	}
}

// processarUmaTarefa desserializa o envelope e despacha ao broker.
func (r *OutboxRelay) processarUmaTarefa(ctx context.Context, t alerta.TarefaOutbox) {
	var env alerta.Envelope
	if err := json.Unmarshal([]byte(t.PayloadJSON), &env); err != nil {
		slog.Error("payload JSON da tarefa outbox inválido", "tarefa_id", t.ID, "alerta_id", t.AlertaID, "erro", err)
		_ = r.repo.IncrementarFalhaTarefa(ctx, t.ID, "json inválido: "+err.Error(), time.Now().Add(24*time.Hour))
		return
	}

	routingKey := t.Topico
	if routingKey == "" {
		routingKey = r.topico
	}

	// Publica no broker com Publisher Confirms
	ctxPub, cancelPub := context.WithTimeout(ctx, 3*time.Second)
	err := r.publicador.Publicar(ctxPub, routingKey, env)
	cancelPub()

	if err != nil {
		proximaTentativa := calcularBackoff(t.Tentativas)
		slog.Warn("falha ao publicar tarefa no rabbitmq; agendando retentativa",
			"tarefa_id", t.ID,
			"alerta_id", t.AlertaID,
			"tentativa", t.Tentativas+1,
			"proxima_em", proximaTentativa.Format(time.RFC3339),
			"erro", err.Error(),
		)
		_ = r.repo.IncrementarFalhaTarefa(ctx, t.ID, err.Error(), proximaTentativa)
		return
	}

	// Sucesso confirmado: marca como 'publicado' no SQLite
	if err := r.repo.MarcarTarefaPublicada(ctx, t.ID); err != nil {
		slog.Error("erro ao marcar tarefa como publicada no sqlite",
			"tarefa_id", t.ID,
			"alerta_id", t.AlertaID,
			"erro", err,
		)
		return
	}

	slog.Info("tarefa outbox publicada e confirmada no rabbitmq",
		"tarefa_id", t.ID,
		"alerta_id", t.AlertaID,
		"evento", env.Evento,
		"id_evento", env.IDEvento,
		"routing_key", routingKey,
	)
}

// Parar interrompe o relay de forma graciosa.
func (r *OutboxRelay) Parar() {
	r.mu.Lock()
	if !r.executando {
		r.mu.Unlock()
		return
	}
	r.executando = false
	r.cancel()
	r.mu.Unlock()

	r.wg.Wait()
	slog.Info("outbox relay finalizado")
}

func calcularBackoff(tentativas int) time.Time {
	// Backoff exponencial: 1s, 2s, 4s, 8s... até 1 minuto
	segundos := math.Pow(2, float64(tentativas))
	if segundos > 60 {
		segundos = 60
	}
	return time.Now().Add(time.Duration(segundos) * time.Second)
}
