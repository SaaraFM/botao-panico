package resiliencia_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/plataforma-mulher/botao-panico/internal/resiliencia"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRateLimiter_BypassQuandoZero(t *testing.T) {
	lim := resiliencia.NovoLimitadorTaxa(0)
	assert.Equal(t, time.Duration(0), lim.Intervalo())

	inicio := time.Now()
	for i := 0; i < 5; i++ {
		err := lim.Aguardar(context.Background())
		require.NoError(t, err)
	}
	assert.Less(t, time.Since(inicio), 20*time.Millisecond)
}

func TestRateLimiter_IntervaloRespeitado(t *testing.T) {
	intervalo := 25 * time.Millisecond
	lim := resiliencia.NovoLimitadorTaxa(intervalo)

	inicio := time.Now()
	// Primeira requisição: imediata
	err := lim.Aguardar(context.Background())
	require.NoError(t, err)

	// Segunda requisição: deve aguardar aproximadamente o intervalo
	err = lim.Aguardar(context.Background())
	require.NoError(t, err)

	decorrido := time.Since(inicio)
	assert.GreaterOrEqual(t, decorrido, 20*time.Millisecond)
}

func TestRateLimiter_CancelamentoContexto(t *testing.T) {
	lim := resiliencia.NovoLimitadorTaxa(500 * time.Millisecond)

	// Primeira requisição consome a janela
	_ = lim.Aguardar(context.Background())

	// Segunda requisição com timeout curto deve cancelar
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	err := lim.Aguardar(ctx)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestRateLimiter_Concorrencia(t *testing.T) {
	intervalo := 5 * time.Millisecond
	lim := resiliencia.NovoLimitadorTaxa(intervalo)

	var wg sync.WaitGroup
	const chamadas = 5
	inicio := time.Now()

	wg.Add(chamadas)
	for i := 0; i < chamadas; i++ {
		go func() {
			defer wg.Done()
			_ = lim.Aguardar(context.Background())
		}()
	}

	wg.Wait()
	assert.GreaterOrEqual(t, time.Since(inicio), 15*time.Millisecond)
}
