package cliente_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/plataforma-mulher/botao-panico/internal/alerta"
	"github.com/plataforma-mulher/botao-panico/internal/cliente"
	"github.com/plataforma-mulher/botao-panico/internal/resiliencia"
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

// -------------------------------------------------------------------------
// Testes do Circuit Breaker no Nominatim (Closed, Open, Half-Open)
// -------------------------------------------------------------------------

func TestNominatim_CircuitBreaker_AberturaPorFalhasConsecutivas(t *testing.T) {
	var requisicoesRecebidas atomic.Int64

	srv := servidorFalso(t, func(w http.ResponseWriter, _ *http.Request) {
		requisicoesRecebidas.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable) // HTTP 503
		_, _ = w.Write([]byte(`{"error":"server overloaded"}`))
	})

	cli := cliente.NovoNominatimComOpcoes(cliente.OpcoesNominatim{
		URLBase:            srv.URL,
		Agente:             "teste-cb",
		Timeout:            500 * time.Millisecond,
		MaxFalhasCB:        2, // abre com 2 falhas
		TimeoutRecuperacao: 50 * time.Millisecond,
		IntervaloMinimo:    0,
	})

	assert.Equal(t, resiliencia.EstadoFechado, cli.CircuitBreaker().Estado())

	// 1ª falha -> atinge o servidor
	_, err := cli.EnderecoAproximado(context.Background(), araranguaCentro)
	assert.ErrorIs(t, err, cliente.ErrServicoIndisponivel)
	assert.Equal(t, int64(1), requisicoesRecebidas.Load())
	assert.Equal(t, resiliencia.EstadoFechado, cli.CircuitBreaker().Estado())

	// 2ª falha -> atinge o limite e abre o circuito
	_, err = cli.EnderecoAproximado(context.Background(), araranguaCentro)
	assert.ErrorIs(t, err, cliente.ErrServicoIndisponivel)
	assert.Equal(t, int64(2), requisicoesRecebidas.Load())
	assert.Equal(t, resiliencia.EstadoAberto, cli.CircuitBreaker().Estado())

	// 3ª chamada -> Circuito está OPEN! Fail-fast imediato sem tocar no servidor
	inicio := time.Now()
	_, err = cli.EnderecoAproximado(context.Background(), araranguaCentro)
	duracao := time.Since(inicio)

	assert.ErrorIs(t, err, cliente.ErrCircuitoAberto)
	assert.Equal(t, int64(2), requisicoesRecebidas.Load(), "nenhuma requisição HTTP deve ter sido feita")
	assert.Less(t, duracao, 10*time.Millisecond, "fail-fast deve ser imediato (0ms)")
}

func TestNominatim_CircuitBreaker_RecuperacaoCanary(t *testing.T) {
	var requisicoesRecebidas atomic.Int64
	var retornarSucesso atomic.Bool

	srv := servidorFalso(t, func(w http.ResponseWriter, _ *http.Request) {
		requisicoesRecebidas.Add(1)
		if retornarSucesso.Load() {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"display_name":"Praça Hercílio Luz, Centro, Araranguá"}`))
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
	})

	cli := cliente.NovoNominatimComOpcoes(cliente.OpcoesNominatim{
		URLBase:            srv.URL,
		Agente:             "teste-recuperacao",
		Timeout:            500 * time.Millisecond,
		MaxFalhasCB:        2,
		TimeoutRecuperacao: 40 * time.Millisecond, // resfriamento curto para o teste
		IntervaloMinimo:    0,
	})

	// Força abertura do circuito
	_, _ = cli.EnderecoAproximado(context.Background(), araranguaCentro)
	_, _ = cli.EnderecoAproximado(context.Background(), araranguaCentro)
	assert.Equal(t, resiliencia.EstadoAberto, cli.CircuitBreaker().Estado())

	// Enquanto no resfriamento, fail-fast
	_, err := cli.EnderecoAproximado(context.Background(), araranguaCentro)
	assert.ErrorIs(t, err, cliente.ErrCircuitoAberto)

	// Aguarda o término da janela de resfriamento
	time.Sleep(50 * time.Millisecond)

	// Simula que o serviço externo voltou
	retornarSucesso.Store(true)

	// Requisição Sonda (Canary): deve transitar para HALF_OPEN e restabelecer CLOSED
	endereco, err := cli.EnderecoAproximado(context.Background(), araranguaCentro)
	require.NoError(t, err)
	assert.Equal(t, "Praça Hercílio Luz, Centro, Araranguá", endereco)
	assert.Equal(t, resiliencia.EstadoFechado, cli.CircuitBreaker().Estado())

	// Próximas chamadas continuam operando normalmente
	endereco2, err2 := cli.EnderecoAproximado(context.Background(), araranguaCentro)
	require.NoError(t, err2)
	assert.Equal(t, "Praça Hercílio Luz, Centro, Araranguá", endereco2)
}

func TestNominatim_CircuitBreaker_CanaryFalha_Reabre(t *testing.T) {
	srv := servidorFalso(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusGatewayTimeout) // HTTP 504
	})

	cli := cliente.NovoNominatimComOpcoes(cliente.OpcoesNominatim{
		URLBase:            srv.URL,
		Agente:             "teste-canary-fail",
		Timeout:            500 * time.Millisecond,
		MaxFalhasCB:        1,
		TimeoutRecuperacao: 30 * time.Millisecond,
		IntervaloMinimo:    0,
	})

	// Abre o circuito
	_, _ = cli.EnderecoAproximado(context.Background(), araranguaCentro)
	assert.Equal(t, resiliencia.EstadoAberto, cli.CircuitBreaker().Estado())

	// Aguarda resfriamento
	time.Sleep(40 * time.Millisecond)

	// Requisição Sonda falha
	_, err := cli.EnderecoAproximado(context.Background(), araranguaCentro)
	assert.ErrorIs(t, err, cliente.ErrServicoIndisponivel)
	assert.Equal(t, resiliencia.EstadoAberto, cli.CircuitBreaker().Estado())

	// Próxima já é fail-fast imediato
	_, err = cli.EnderecoAproximado(context.Background(), araranguaCentro)
	assert.ErrorIs(t, err, cliente.ErrCircuitoAberto)
}

func TestNominatim_RateLimiter_IntervaloRespeitado(t *testing.T) {
	srv := servidorFalso(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"display_name":"Rua Teste"}`))
	})

	cli := cliente.NovoNominatimComOpcoes(cliente.OpcoesNominatim{
		URLBase:            srv.URL,
		Agente:             "teste-rate",
		Timeout:            time.Second,
		MaxFalhasCB:        3,
		TimeoutRecuperacao: time.Second,
		IntervaloMinimo:    25 * time.Millisecond,
	})

	inicio := time.Now()
	// Chamada 1
	_, err := cli.EnderecoAproximado(context.Background(), araranguaCentro)
	require.NoError(t, err)

	// Chamada 2 deve respeitar o espaçamento do rate limiter
	_, err = cli.EnderecoAproximado(context.Background(), araranguaCentro)
	require.NoError(t, err)

	assert.GreaterOrEqual(t, time.Since(inicio), 20*time.Millisecond)
}

func TestNominatim_CircuitBreaker_Concorrencia(t *testing.T) {
	srv := servidorFalso(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})

	cli := cliente.NovoNominatimComOpcoes(cliente.OpcoesNominatim{
		URLBase:            srv.URL,
		Agente:             "teste-concorrencia",
		Timeout:            500 * time.Millisecond,
		MaxFalhasCB:        3,
		TimeoutRecuperacao: 30 * time.Millisecond,
		IntervaloMinimo:    0,
	})

	var wg sync.WaitGroup
	const goroutines = 25

	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			_, _ = cli.EnderecoAproximado(context.Background(), araranguaCentro)
		}()
	}

	wg.Wait()
	assert.Equal(t, resiliencia.EstadoAberto, cli.CircuitBreaker().Estado())
}
