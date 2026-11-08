package resiliencia

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

var (
	// ErrCircuitoAberto é retornado imediatamente quando o circuit breaker está no estado OPEN
	// ou quando uma requisição secundária tenta executar durante o estado HALF_OPEN.
	ErrCircuitoAberto = errors.New("circuit breaker aberto: requisição bloqueada preventivamente (fail-fast)")
)

// EstadoCircuito representa os 3 estados canônicos do Circuit Breaker:
//   - CLOSED: fluxo normal, chamadas passam diretamente;
//   - OPEN: circuito interrompido após falhas consecutivas, fail-fast em 0ms;
//   - HALF_OPEN: período de resfriamento expirou, permite 1 requisição sonda para teste.
type EstadoCircuito string

const (
	EstadoFechado    EstadoCircuito = "CLOSED"
	EstadoAberto     EstadoCircuito = "OPEN"
	EstadoSemiaberto EstadoCircuito = "HALF_OPEN"
)

func (e EstadoCircuito) String() string {
	return string(e)
}

// OpcoesCircuitBreaker reúne os parâmetros de inicialização do Circuit Breaker.
type OpcoesCircuitBreaker struct {
	Nome               string
	MaxFalhas          int
	TimeoutRecuperacao time.Duration
	Classificador      func(error) bool
	AoMudarEstado      func(nome string, anterior, novo EstadoCircuito)
	AoRejeitar         func(nome string)
}

// CircuitBreaker implementa proteção fail-fast thread-safe com os 3 estados
// (Closed, Open e Half-Open) e logs estruturados em cada transição.
type CircuitBreaker struct {
	mu                 sync.RWMutex
	nome               string
	estado             EstadoCircuito
	maxFalhas          int
	timeoutRecuperacao time.Duration
	falhasConsecutivas int
	expiraEm           time.Time
	sondaEmExecucao    bool
	classificador      func(error) bool
	aoMudarEstado      func(nome string, anterior, novo EstadoCircuito)
	aoRejeitar         func(nome string)
}

// NovoCircuitBreaker instancia um novo Circuit Breaker configurado no estado CLOSED.
func NovoCircuitBreaker(opts OpcoesCircuitBreaker) *CircuitBreaker {
	nome := opts.Nome
	if nome == "" {
		nome = "padrao"
	}
	maxFalhas := opts.MaxFalhas
	if maxFalhas <= 0 {
		maxFalhas = 3
	}
	timeoutRecuperacao := opts.TimeoutRecuperacao
	if timeoutRecuperacao <= 0 {
		timeoutRecuperacao = 10 * time.Second
	}

	return &CircuitBreaker{
		nome:               nome,
		estado:             EstadoFechado,
		maxFalhas:          maxFalhas,
		timeoutRecuperacao: timeoutRecuperacao,
		classificador:      opts.Classificador,
		aoMudarEstado:      opts.AoMudarEstado,
		aoRejeitar:         opts.AoRejeitar,
	}
}

// Executar executa a operação `fn` protegida pelo Circuit Breaker.
// Se o circuito estiver OPEN, rejeita imediatamente com ErrCircuitoAberto em 0ms.
// Se estiver HALF_OPEN, permite apenas 1 sonda; requisições concorrentes recebem ErrCircuitoAberto.
func (cb *CircuitBreaker) Executar(ctx context.Context, fn func() error) error {
	// 1. Fase de inspeção e decisão sob lock
	cb.mu.Lock()
	agora := time.Now()

	switch cb.estado {
	case EstadoAberto:
		if agora.After(cb.expiraEm) {
			// Janela de resfriamento expirou: transição para HALF_OPEN
			anterior := cb.estado
			cb.estado = EstadoSemiaberto
			cb.sondaEmExecucao = true
			cb.notificarTransicao(anterior, EstadoSemiaberto, "janela de resfriamento expirou; liberando requisição sonda")
			cb.mu.Unlock()
		} else {
			// Continua aberto: fail-fast imediato sem tocar no serviço externo
			if cb.aoRejeitar != nil {
				cb.aoRejeitar(cb.nome)
			}
			slog.Warn("circuit breaker: requisição rejeitada em fail-fast (circuito aberto)",
				"nome", cb.nome,
				"estado", cb.estado,
				"resfriamento_restante_ms", cb.expiraEm.Sub(agora).Milliseconds(),
			)
			cb.mu.Unlock()
			return ErrCircuitoAberto
		}

	case EstadoSemiaberto:
		if cb.sondaEmExecucao {
			// Já existe uma requisição de sonda sendo avaliada; bloqueia as demais para não sobrecarregar
			if cb.aoRejeitar != nil {
				cb.aoRejeitar(cb.nome)
			}
			slog.Warn("circuit breaker: requisição rejeitada em fail-fast (sonda já em andamento)",
				"nome", cb.nome,
				"estado", cb.estado,
			)
			cb.mu.Unlock()
			return ErrCircuitoAberto
		}
		cb.sondaEmExecucao = true
		cb.mu.Unlock()

	case EstadoFechado:
		cb.mu.Unlock()

	default:
		cb.mu.Unlock()
	}

	// 2. Execução da chamada protegida fora do lock (permite que outras goroutines inspecionem)
	err := fn()

	// 3. Atualização de estado após retorno sob lock
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.estado {
	case EstadoSemiaberto:
		cb.sondaEmExecucao = false
		if err != nil && cb.ehFalha(err) {
			// Sonda falhou: reabre o circuito por novo período de resfriamento
			anterior := cb.estado
			cb.estado = EstadoAberto
			cb.expiraEm = time.Now().Add(cb.timeoutRecuperacao)
			cb.notificarTransicao(anterior, EstadoAberto, fmt.Sprintf("requisição sonda falhou (%v); reiniciando resfriamento", err))
		} else {
			// Sonda teve sucesso: restabelece CLOSED e zera falhas
			anterior := cb.estado
			cb.estado = EstadoFechado
			cb.falhasConsecutivas = 0
			cb.notificarTransicao(anterior, EstadoFechado, "requisição sonda concluída com sucesso; circuito normalizado")
		}

	case EstadoFechado:
		if err != nil && cb.ehFalha(err) {
			cb.falhasConsecutivas++
			slog.Warn("circuit breaker: falha registrada",
				"nome", cb.nome,
				"falhas_consecutivas", cb.falhasConsecutivas,
				"max_falhas", cb.maxFalhas,
				"erro", err.Error(),
			)
			if cb.falhasConsecutivas >= cb.maxFalhas {
				anterior := cb.estado
				cb.estado = EstadoAberto
				cb.expiraEm = time.Now().Add(cb.timeoutRecuperacao)
				cb.notificarTransicao(anterior, EstadoAberto, fmt.Sprintf("limite de %d falhas consecutivas atingido", cb.maxFalhas))
			}
		} else if err == nil {
			if cb.falhasConsecutivas > 0 {
				cb.falhasConsecutivas = 0
			}
		}

	case EstadoAberto:
		// Se já foi aberto por outra goroutine, mantém
	}

	return err
}

func (cb *CircuitBreaker) ehFalha(err error) bool {
	if err == nil {
		return false
	}
	if cb.classificador != nil {
		return cb.classificador(err)
	}
	return true
}

func (cb *CircuitBreaker) notificarTransicao(de, para EstadoCircuito, motivo string) {
	slog.Info("circuit breaker: transição de estado",
		"nome", cb.nome,
		"estado_anterior", de.String(),
		"novo_estado", para.String(),
		"motivo", motivo,
	)
	if cb.aoMudarEstado != nil {
		cb.aoMudarEstado(cb.nome, de, para)
	}
}

// Estado retorna o estado atual do circuito de forma thread-safe.
func (cb *CircuitBreaker) Estado() EstadoCircuito {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.estado
}

// FalhasConsecutivas retorna a contagem atual de falhas consecutivas.
func (cb *CircuitBreaker) FalhasConsecutivas() int {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.falhasConsecutivas
}

// Resetar força o restabelecimento do circuito para o estado CLOSED com contador zerado.
func (cb *CircuitBreaker) Resetar() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	anterior := cb.estado
	cb.estado = EstadoFechado
	cb.falhasConsecutivas = 0
	cb.sondaEmExecucao = false
	if anterior != EstadoFechado {
		cb.notificarTransicao(anterior, EstadoFechado, "reset manual do circuito")
	}
}
