package alerta_test

import (
	"testing"

	"github.com/plataforma-mulher/botao-panico/internal/alerta"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// pedidoValido é um pedido base correto; cada teste "quebra" um campo dele.
func pedidoValido() alerta.PedidoAlerta {
	return alerta.PedidoAlerta{
		UsuariaID: "usuaria-demo",
		Nivel:     alerta.RiscoAlto,
		Mensagem:  "preciso de ajuda agora",
		Local:     alerta.Localizacao{Latitude: -28.9346, Longitude: -49.4957},
	}
}

func TestValidar_PedidoCorreto(t *testing.T) {
	pedido := pedidoValido()

	err := pedido.Validar()

	assert.NoError(t, err)
}

func TestValidar_UsuariaObrigatoria(t *testing.T) {
	pedido := pedidoValido()
	pedido.UsuariaID = ""

	err := pedido.Validar()

	require.Error(t, err)
	erros := err.(*alerta.ErrosValidacao)
	assert.Contains(t, erros.Campos, "usuaria_id")
}

func TestValidar_NivelInvalido(t *testing.T) {
	pedido := pedidoValido()
	pedido.Nivel = "urgentissimo"

	err := pedido.Validar()

	require.Error(t, err)
	assert.Contains(t, err.(*alerta.ErrosValidacao).Campos, "nivel")
}

func TestValidar_LocalizacaoObrigatoria(t *testing.T) {
	pedido := pedidoValido()
	pedido.Local = alerta.Localizacao{} // (0,0) = não capturada

	err := pedido.Validar()

	require.Error(t, err)
	assert.Contains(t, err.(*alerta.ErrosValidacao).Campos, "local")
}

func TestValidar_LatitudeForaDaFaixa(t *testing.T) {
	pedido := pedidoValido()
	pedido.Local.Latitude = -95

	err := pedido.Validar()

	require.Error(t, err)
	assert.Contains(t, err.(*alerta.ErrosValidacao).Campos, "local.latitude")
}

// TestValidar_TabelaDeCasos usa o padrão table-driven tests do Go.
func TestValidar_TabelaDeCasos(t *testing.T) {
	casos := []struct {
		nome        string
		alterar     func(*alerta.PedidoAlerta)
		campoEspera string
	}{
		{"mensagem longa demais", func(p *alerta.PedidoAlerta) {
			p.Mensagem = string(make([]byte, 501))
		}, "mensagem"},
		{"longitude fora da faixa", func(p *alerta.PedidoAlerta) {
			p.Local.Longitude = 200
		}, "local.longitude"},
	}

	for _, tc := range casos {
		t.Run(tc.nome, func(t *testing.T) {
			pedido := pedidoValido()
			tc.alterar(&pedido)

			err := pedido.Validar()

			require.Error(t, err)
			assert.Contains(t, err.(*alerta.ErrosValidacao).Campos, tc.campoEspera)
		})
	}
}

func TestExigeInstituicoes(t *testing.T) {
	assert.True(t, (&alerta.PedidoAlerta{Nivel: alerta.RiscoAlto}).ExigeInstituicoes())
	assert.True(t, (&alerta.PedidoAlerta{Nivel: alerta.RiscoCritico}).ExigeInstituicoes())
	assert.False(t, (&alerta.PedidoAlerta{Nivel: alerta.RiscoBaixo}).ExigeInstituicoes())
	assert.False(t, (&alerta.PedidoAlerta{Nivel: alerta.RiscoModerado}).ExigeInstituicoes())
}
