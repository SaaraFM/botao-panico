package cliente_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/plataforma-mulher/botao-panico/internal/alerta"
	"github.com/plataforma-mulher/botao-panico/internal/cliente"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var araranguaCentro = alerta.Localizacao{Latitude: -28.9346, Longitude: -49.4957}

func servidorFalso(t *testing.T, h http.HandlerFunc) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv
}

func TestNominatim_Sucesso(t *testing.T) {
	srv := servidorFalso(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/reverse", r.URL.Path)
		assert.Equal(t, "-28.934600", r.URL.Query().Get("lat"))
		assert.Equal(t, "-49.495700", r.URL.Query().Get("lon"))
		assert.Equal(t, "jsonv2", r.URL.Query().Get("format"))
		assert.Equal(t, "agente-teste", r.Header.Get("User-Agent"))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"display_name":"Avenida Getúlio Vargas, Centro, Araranguá, SC, Brasil"}`))
	})

	endereco, err := cliente.NovoNominatim(srv.URL, "agente-teste", time.Second).
		EnderecoAproximado(context.Background(), araranguaCentro)

	require.NoError(t, err)
	assert.Equal(t, "Avenida Getúlio Vargas, Centro, Araranguá, SC, Brasil", endereco)
}

func TestNominatim_Falhas(t *testing.T) {
	casos := []struct {
		nome     string
		status   int
		corpo    string
		esperado error
	}{
		{"HTTP 400", http.StatusBadRequest, `{"error":"bad"}`, cliente.ErrRequisicaoRejeitada},
		{"HTTP 429", http.StatusTooManyRequests, ``, cliente.ErrServicoIndisponivel},
		{"HTTP 500", http.StatusInternalServerError, `oops`, cliente.ErrServicoIndisponivel},
		{"HTTP 204 inesperado", http.StatusNoContent, ``, cliente.ErrRespostaInvalida},
		{"JSON inválido", http.StatusOK, `{"display_name":`, cliente.ErrRespostaInvalida},
		{"sem display_name", http.StatusOK, `{"lat":"1"}`, cliente.ErrRespostaInvalida},
		{"erro no corpo", http.StatusOK, `{"error":"Unable to geocode"}`, cliente.ErrRespostaInvalida},
		{"tipo inesperado", http.StatusOK, `["lista"]`, cliente.ErrRespostaInvalida},
	}
	for _, c := range casos {
		t.Run(c.nome, func(t *testing.T) {
			srv := servidorFalso(t, func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(c.status)
				_, _ = w.Write([]byte(c.corpo))
			})
			_, err := cliente.NovoNominatim(srv.URL, "teste", time.Second).
				EnderecoAproximado(context.Background(), araranguaCentro)
			assert.ErrorIs(t, err, c.esperado)
		})
	}
}

func TestNominatim_Timeout(t *testing.T) {
	srv := servidorFalso(t, func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(2 * time.Second):
		case <-r.Context().Done():
		}
	})

	inicio := time.Now()
	_, err := cliente.NovoNominatim(srv.URL, "teste", 100*time.Millisecond).
		EnderecoAproximado(context.Background(), araranguaCentro)

	assert.ErrorIs(t, err, cliente.ErrTimeout)
	assert.Less(t, time.Since(inicio), time.Second, "o timeout foi respeitado")
}

func TestNominatim_ContextoComPrazo(t *testing.T) {
	srv := servidorFalso(t, func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	})
	ctx, cancelar := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancelar()

	_, err := cliente.NovoNominatim(srv.URL, "teste", 5*time.Second).EnderecoAproximado(ctx, araranguaCentro)
	assert.ErrorIs(t, err, cliente.ErrTimeout)
}

func TestNominatim_FalhaDeConexao(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close() // nada mais escuta nesta porta

	_, err := cliente.NovoNominatim(url, "teste", time.Second).
		EnderecoAproximado(context.Background(), araranguaCentro)
	assert.ErrorIs(t, err, cliente.ErrServicoIndisponivel)
}
