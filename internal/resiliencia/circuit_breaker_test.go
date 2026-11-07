package resiliencia_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/plataforma-mulher/botao-panico/internal/resiliencia"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCircuitBreaker_FluxoSucesso(t *testing.T) {
	cb := resiliencia.NovoCircuitBreaker(resiliencia.OpcoesCircuitBreaker{
		Nome:               "teste-sucesso",
		MaxFalhas:          3,
		TimeoutRecuperacao: 100 * time.Millisecond,
	})

	assert.Equal(t, resiliencia.EstadoFechado, cb.Estado())

	err := cb.Executar(context.Background(), func() error {
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, resiliencia.EstadoFechado, cb.Estado())
	assert.Equal(t, 0, cb.FalhasConsecutivas())
}

func TestCircuitBreaker_TransicaoClosedParaOpen(t *testing.T) {
	transicoes := make([]string, 0)
	var mu sync.Mutex

	cb := resiliencia.NovoCircuitBreaker(resiliencia.OpcoesCircuitBreaker{
		Nome:               "teste-closed-to-open",
		MaxFalhas:          3,
		TimeoutRecuperacao: 50 * time.Millisecond,
		AoMudarEstado: func(nome string, ant, novo resiliencia.EstadoCircuito) {
			mu.Lock()
			transicoes = append(transicoes, string(ant)+"->"+string(novo))
			mu.Unlock()
		},
	})

	errSimulado := errors.New("falha externa 500")

	// 1ª falha
	_ = cb.Executar(context.Background(), func() error { return errSimulado })
	assert.Equal(t, resiliencia.EstadoFechado, cb.Estado())
	assert.Equal(t, 1, cb.FalhasConsecutivas())

	// 2ª falha
	_ = cb.Executar(context.Background(), func() error { return errSimulado })
	assert.Equal(t, resiliencia.EstadoFechado, cb.Estado())
	assert.Equal(t, 2, cb.FalhasConsecutivas())

	// 3ª falha -> Deve abrir o circuito!
	_ = cb.Executar(context.Background(), func() error { return errSimulado })
	assert.Equal(t, resiliencia.EstadoAberto, cb.Estado())

	// 4ª chamada -> Fail-fast sem executar a função
	executou := false
	inicio := time.Now()
	err := cb.Executar(context.Background(), func() error {
		executou = true
		return nil
	})
	duracao := time.Since(inicio)

	assert.False(t, executou, "a função não deveria ter sido chamada com o circuito aberto")
	assert.ErrorIs(t, err, resiliencia.ErrCircuitoAberto)
	assert.Less(t, duracao, 10*time.Millisecond, "fail-fast deve ser quase instantâneo")

	mu.Lock()
	assert.Contains(t, transicoes, "CLOSED->OPEN")
	mu.Unlock()
}

func TestCircuitBreaker_RecuperacaoHalfOpenParaClosed(t *testing.T) {
	transicoes := make([]string, 0)
	var mu sync.Mutex

	cb := resiliencia.NovoCircuitBreaker(resiliencia.OpcoesCircuitBreaker{
		Nome:               "teste-recuperacao",
		MaxFalhas:          2,
		TimeoutRecuperacao: 40 * time.Millisecond,
		AoMudarEstado: func(nome string, ant, novo resiliencia.EstadoCircuito) {
			mu.Lock()
			transicoes = append(transicoes, string(ant)+"->"+string(novo))
			mu.Unlock()
		},
	})

	// Força abertura
	_ = cb.Executar(context.Background(), func() error { return errors.New("falha 1") })
	_ = cb.Executar(context.Background(), func() error { return errors.New("falha 2") })
	assert.Equal(t, resiliencia.EstadoAberto, cb.Estado())

	// Aguarda tempo de resfriamento
	time.Sleep(50 * time.Millisecond)

	// Próxima requisição é a sonda (canary): simula retorno do serviço externo com sucesso
	sondaExecutou := false
	err := cb.Executar(context.Background(), func() error {
		sondaExecutou = true
		return nil
	})

	require.NoError(t, err)
	assert.True(t, sondaExecutou)
	assert.Equal(t, resiliencia.EstadoFechado, cb.Estado())
	assert.Equal(t, 0, cb.FalhasConsecutivas())

	mu.Lock()
	assert.Contains(t, transicoes, "CLOSED->OPEN")
	assert.Contains(t, transicoes, "OPEN->HALF_OPEN")
	assert.Contains(t, transicoes, "HALF_OPEN->CLOSED")
	mu.Unlock()
}

func TestCircuitBreaker_RecuperacaoHalfOpenParaReOpen(t *testing.T) {
	cb := resiliencia.NovoCircuitBreaker(resiliencia.OpcoesCircuitBreaker{
		Nome:               "teste-falha-canary",
		MaxFalhas:          2,
		TimeoutRecuperacao: 30 * time.Millisecond,
	})

	// Força abertura
	_ = cb.Executar(context.Background(), func() error { return errors.New("falha 1") })
	_ = cb.Executar(context.Background(), func() error { return errors.New("falha 2") })
	assert.Equal(t, resiliencia.EstadoAberto, cb.Estado())

	// Aguarda expiração do resfriamento
	time.Sleep(40 * time.Millisecond)

	// Sonda falha
	errSonda := errors.New("serviço ainda instável")
	err := cb.Executar(context.Background(), func() error {
		return errSonda
	})

	assert.ErrorIs(t, err, errSonda)
	assert.Equal(t, resiliencia.EstadoAberto, cb.Estado(), "circuito deve reabrir imediatamente após falha na sonda")

	// Chamada logo em seguida é bloqueada
	errImediato := cb.Executar(context.Background(), func() error { return nil })
	assert.ErrorIs(t, errImediato, resiliencia.ErrCircuitoAberto)
}

func TestCircuitBreaker_ClassificadorDeErro(t *testing.T) {
	errInfra := errors.New("503 indisponível")
	errValidacao := errors.New("400 bad request")

	cb := resiliencia.NovoCircuitBreaker(resiliencia.OpcoesCircuitBreaker{
		Nome:               "teste-classificador",
		MaxFalhas:          2,
		TimeoutRecuperacao: 100 * time.Millisecond,
		Classificador: func(err error) bool {
			return errors.Is(err, errInfra)
		},
	})

	// Erro de validação não deve incrementar falhas
	_ = cb.Executar(context.Background(), func() error { return errValidacao })
	_ = cb.Executar(context.Background(), func() error { return errValidacao })
	assert.Equal(t, 0, cb.FalhasConsecutivas())
	assert.Equal(t, resiliencia.EstadoFechado, cb.Estado())

	// Erro de infra incrementa falhas
	_ = cb.Executar(context.Background(), func() error { return errInfra })
	assert.Equal(t, 1, cb.FalhasConsecutivas())
	_ = cb.Executar(context.Background(), func() error { return errInfra })
	assert.Equal(t, resiliencia.EstadoAberto, cb.Estado())
}

func TestCircuitBreaker_Concorrencia(t *testing.T) {
	cb := resiliencia.NovoCircuitBreaker(resiliencia.OpcoesCircuitBreaker{
		Nome:               "teste-concorrencia",
		MaxFalhas:          5,
		TimeoutRecuperacao: 30 * time.Millisecond,
	})

	const goroutines = 30
	var wg sync.WaitGroup
	var bloqueados atomic.Int64
	var executados atomic.Int64

	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			err := cb.Executar(context.Background(), func() error {
				executados.Add(1)
				if idx%2 == 0 {
					return errors.New("falha par")
				}
				return nil
			})
			if errors.Is(err, resiliencia.ErrCircuitoAberto) {
				bloqueados.Add(1)
			}
		}(i)
	}

	wg.Wait()
	assert.True(t, executados.Load() > 0)
}

func TestCircuitBreaker_Reset(t *testing.T) {
	cb := resiliencia.NovoCircuitBreaker(resiliencia.OpcoesCircuitBreaker{
		Nome:               "teste-reset",
		MaxFalhas:          1,
		TimeoutRecuperacao: 1 * time.Minute,
	})

	_ = cb.Executar(context.Background(), func() error { return errors.New("erro") })
	assert.Equal(t, resiliencia.EstadoAberto, cb.Estado())

	cb.Resetar()
	assert.Equal(t, resiliencia.EstadoFechado, cb.Estado())
	assert.Equal(t, 0, cb.FalhasConsecutivas())

	err := cb.Executar(context.Background(), func() error { return nil })
	require.NoError(t, err)
}
