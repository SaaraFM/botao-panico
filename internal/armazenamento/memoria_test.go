package armazenamento_test

import (
	"context"
	"testing"

	"github.com/plataforma-mulher/botao-panico/internal/alerta"
	"github.com/plataforma-mulher/botao-panico/internal/armazenamento"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemoria_DevolveCopias(t *testing.T) {
	ctx := context.Background()
	m := armazenamento.NovaMemoria()
	a := alertaExemplo("a1")
	require.NoError(t, m.SalvarAlerta(ctx, a))

	lido, err := m.BuscarAlerta(ctx, "a1")
	require.NoError(t, err)
	lido.InstituicoesAlvo[0].Nome = "alterado"
	lido.Status = alerta.StatusConcluido
	assert.Equal(t, alerta.StatusConcluido, lido.Status)

	denovo, err := m.BuscarAlerta(ctx, "a1")
	require.NoError(t, err)
	assert.Equal(t, a, denovo, "alterar a cópia não altera o armazenado")
}

func TestMemoria_ErrosDeNegocio(t *testing.T) {
	ctx := context.Background()
	m := armazenamento.NovaMemoria()
	_, err := m.BuscarAlerta(ctx, "x")
	assert.ErrorIs(t, err, alerta.ErrNaoEncontrado)
	assert.ErrorIs(t, m.AtualizarAlerta(ctx, alertaExemplo("x")), alerta.ErrNaoEncontrado)
	require.NoError(t, m.SalvarAlerta(ctx, alertaExemplo("x")))
	assert.ErrorIs(t, m.SalvarAlerta(ctx, alertaExemplo("x")), alerta.ErrEscrita)
}
