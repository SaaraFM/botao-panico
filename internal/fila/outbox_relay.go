package fila

import (
	"context"
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/plataforma-mulher/botao-panico/internal/alerta"
)

// OutboxRelay é o componente em background que lê tarefas persistidas na tabela 'tarefas_outbox'
// e as publica de forma confiável no NATS JetStream, garantindo entrega 'at-least-once'.
type OutboxRelay struct {
	repo       alerta.RepositorioOutbox
	js         nats.JetStreamContext
	topico     string
	intervalo  time.Duration
	gatilhoCh  chan struct{}
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	mu         sync.Mutex
	executando bool
}

// NovoOutboxRelay inicializa o publisher do Outbox.
func NovoOutboxRelay(
	repo alerta.RepositorioOutbox,
	js nats.JetStreamContext,
	topicoPadrao string,
	intervaloVarredura ...time.Duration,
) *OutboxRelay {
	intervalo := 250 * time.Millisecond
	if len(intervaloVarredura) > 0 && intervaloVarredura[0] > 0 {
		intervalo = intervaloVarredura[0]
	}

	if topicoPadrao == "" {
		topicoPadrao = "alertas.notificar"
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &OutboxRelay{
		repo:      repo,
		js:        js,
		topico:    topicoPadrao,
		intervalo: intervalo,
		gatilhoCh: make(chan struct{}, 1),
		ctx:       ctx,
		cancel:    cancel,
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

	// Processa tarefas pendentes que possam ter ficado de execuções anteriores (recuperação pós-reinicialização)
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

// processarLote busca tarefas pendentes no SQLite e as publica no NATS JetStream.
func (r *OutboxRelay) processarLote(ctx context.Context) {
	tarefas, err := r.repo.BuscarTarefasPendentes(ctx, 50)
	if err != nil {
		slog.Error("outbox relay: falha ao buscar tarefas pendentes", "erro", err)
		return
	}

	for _, t := range tarefas {
		if ctx.Err() != nil {
			return
		}

		topico := t.Topico
		if topico == "" {
			topico = r.topico
		}

		// Publicação no NATS JetStream com Nats-Msg-Id para garantir deduplicação nativa
		msg := &nats.Msg{
			Subject: topico,
			Header: nats.Header{
				"Nats-Msg-Id": []string{t.ID},
			},
			Data: []byte(t.PayloadJSON),
		}

		pubAck, err := r.js.PublishMsg(msg)
		if err != nil {
			// Falha na publicação: calcula backoff exponencial e registra tentativa
			backoff := calcularBackoff(t.Tentativas)
			proxTentativa := time.Now().Add(backoff)
			_ = r.repo.IncrementarFalhaTarefa(ctx, t.ID, err.Error(), proxTentativa)
			slog.Warn("outbox relay: falha ao publicar tarefa no nats",
				"tarefa_id", t.ID, "alerta_id", t.AlertaID, "erro", err, "proxima_tentativa", proxTentativa)
			continue
		}

		// Publicação confirmada pelo broker (Ack recebido): atualiza estado da tarefa no banco
		if pubAck != nil {
			if err := r.repo.MarcarTarefaPublicada(ctx, t.ID); err != nil {
				slog.Error("outbox relay: falha ao marcar tarefa como publicada", "tarefa_id", t.ID, "erro", err)
			}
		}
	}
}

// Parar encerra o relay de forma graciosa.
func (r *OutboxRelay) Parar() {
	r.cancel()
	r.wg.Wait()
	r.mu.Lock()
	r.executando = false
	r.mu.Unlock()
}

// calcularBackoff calcula espera exponencial com jitter básico (ex.: 500ms, 1s, 2s, 4s, 8s).
func calcularBackoff(tentativas int) time.Duration {
	base := 500 * time.Millisecond
	if tentativas > 6 {
		tentativas = 6
	}
	fator := math.Pow(2, float64(tentativas))
	return time.Duration(float64(base) * fator)
}
