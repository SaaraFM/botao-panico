package notificacao_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/plataforma-mulher/botao-panico/internal/alerta"
	"github.com/plataforma-mulher/botao-panico/internal/notificacao"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// notificadorLento simula um canal com latência alta, para provar que
// os envios acontecem em paralelo (e não um após o outro).
type notificadorLento struct{ atraso time.Duration }

func (n notificadorLento) Canal() string { return "sms" }
func (n notificadorLento) Enviar(_ context.Context, _, _ string) error {
	time.Sleep(n.atraso)
	return nil
}

// notificadorComFalha sempre falha, para testar o registro de erros.
type notificadorComFalha struct{}

func (notificadorComFalha) Canal() string { return "sms" }
func (notificadorComFalha) Enviar(_ context.Context, _, _ string) error {
	return fmt.Errorf("operadora indisponível")
}

func destinatariosExemplo(n int) []alerta.Destinatario {
	lista := make([]alerta.Destinatario, n)
	for i := range lista {
		lista[i] = alerta.Destinatario{
			Nome:     fmt.Sprintf("Destinatário %d", i+1),
			Telefone: "+5548999990000",
		}
	}
	return lista
}

func TestDespachar_EnviaParaTodosEmParalelo(t *testing.T) {
	// 10 destinatários × 100ms: em sequência levaria ~1s.
	// Em paralelo, deve terminar bem perto de 100ms.
	desp := notificacao.NovoDespachante(notificadorLento{atraso: 100 * time.Millisecond})

	inicio := time.Now()
	resultados := desp.Despachar(context.Background(), destinatariosExemplo(10), "alerta!")
	duracao := time.Since(inicio)

	require.Len(t, resultados, 10)
	for _, r := range resultados {
		assert.True(t, r.Sucesso)
	}
	assert.Less(t, duracao, 500*time.Millisecond,
		"os envios deveriam acontecer em paralelo, não em sequência")
}

func TestDespachar_RegistraFalhasSemDerrubarOsDemais(t *testing.T) {
	desp := notificacao.NovoDespachante(notificadorComFalha{})

	resultados := desp.Despachar(context.Background(), destinatariosExemplo(3), "alerta!")

	require.Len(t, resultados, 3)
	for _, r := range resultados {
		assert.False(t, r.Sucesso)
		assert.Equal(t, "operadora indisponível", r.Erro)
	}
}

func TestDespachar_RespeitaCancelamentoDoContexto(t *testing.T) {
	desp := notificacao.NovoDespachante(notificacao.SMSNotificador{})

	ctx, cancelar := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancelar()

	resultados := desp.Despachar(ctx, destinatariosExemplo(2), "alerta!")

	require.Len(t, resultados, 2)
	for _, r := range resultados {
		assert.False(t, r.Sucesso) // contexto expirou antes do envio simulado
		assert.NotEmpty(t, r.Erro)
	}
}
