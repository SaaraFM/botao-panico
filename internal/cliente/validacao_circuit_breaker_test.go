package cliente_test

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/plataforma-mulher/botao-panico/internal/alerta"
	"github.com/plataforma-mulher/botao-panico/internal/armazenamento"
	"github.com/plataforma-mulher/botao-panico/internal/cliente"
	"github.com/plataforma-mulher/botao-panico/internal/fila"
	"github.com/plataforma-mulher/botao-panico/internal/notificacao"
	"github.com/plataforma-mulher/botao-panico/internal/resiliencia"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// -------------------------------------------------------------------------
// Validação 1: Circuito CLOSED - requisições normais funcionam
// -------------------------------------------------------------------------
func TestValidacao_1_CircuitoClosed_Sucesso(t *testing.T) {
	srv := servidorFalso(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"display_name":"Avenida Sete de Setembro, Centro, Araranguá, SC"}`))
	})

	cli := cliente.NovoNominatimComOpcoes(cliente.OpcoesNominatim{
		URLBase:            srv.URL,
		Agente:             "validacao-cb-1",
		Timeout:            time.Second,
		MaxFalhasCB:        3,
		TimeoutRecuperacao: time.Second,
		IntervaloMinimo:    0,
	})

	assert.Equal(t, resiliencia.EstadoFechado, cli.CircuitBreaker().Estado())

	end, err := cli.EnderecoAproximado(context.Background(), araranguaCentro)
	require.NoError(t, err)
	assert.Equal(t, "Avenida Sete de Setembro, Centro, Araranguá, SC", end)
	assert.Equal(t, resiliencia.EstadoFechado, cli.CircuitBreaker().Estado())
	assert.Equal(t, 0, cli.CircuitBreaker().FalhasConsecutivas())
}

// -------------------------------------------------------------------------
// Validação 2: Circuito OPEN - após 3 falhas, novas chamadas externas são interrompidas
// -------------------------------------------------------------------------
func TestValidacao_2_CircuitoOpen_Apos3Falhas_FailFast(t *testing.T) {
	var requisicoesExternas atomic.Int64

	srv := servidorFalso(t, func(w http.ResponseWriter, _ *http.Request) {
		requisicoesExternas.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable) // HTTP 503
	})

	cli := cliente.NovoNominatimComOpcoes(cliente.OpcoesNominatim{
		URLBase:            srv.URL,
		Agente:             "validacao-cb-2",
		Timeout:            500 * time.Millisecond,
		MaxFalhasCB:        3, // Limite exato de 3 falhas
		TimeoutRecuperacao: 80 * time.Millisecond,
		IntervaloMinimo:    0,
	})

	// 1ª falha
	_, err := cli.EnderecoAproximado(context.Background(), araranguaCentro)
	assert.ErrorIs(t, err, cliente.ErrServicoIndisponivel)
	assert.Equal(t, resiliencia.EstadoFechado, cli.CircuitBreaker().Estado())
	assert.Equal(t, 1, cli.CircuitBreaker().FalhasConsecutivas())

	// 2ª falha
	_, err = cli.EnderecoAproximado(context.Background(), araranguaCentro)
	assert.ErrorIs(t, err, cliente.ErrServicoIndisponivel)
	assert.Equal(t, resiliencia.EstadoFechado, cli.CircuitBreaker().Estado())
	assert.Equal(t, 2, cli.CircuitBreaker().FalhasConsecutivas())

	// 3ª falha -> Transita para OPEN!
	_, err = cli.EnderecoAproximado(context.Background(), araranguaCentro)
	assert.ErrorIs(t, err, cliente.ErrServicoIndisponivel)
	assert.Equal(t, resiliencia.EstadoAberto, cli.CircuitBreaker().Estado())
	assert.Equal(t, int64(3), requisicoesExternas.Load())

	// 4ª e 5ª chamadas: Devem sofrer FAIL-FAST imediato sem tocar no servidor simulado
	inicio := time.Now()
	_, err4 := cli.EnderecoAproximado(context.Background(), araranguaCentro)
	_, err5 := cli.EnderecoAproximado(context.Background(), araranguaCentro)
	duracao := time.Since(inicio)

	assert.ErrorIs(t, err4, cliente.ErrCircuitoAberto)
	assert.ErrorIs(t, err5, cliente.ErrCircuitoAberto)
	assert.Equal(t, int64(3), requisicoesExternas.Load(), "o servidor não deve ter recebido novas chamadas com o circuito aberto")
	assert.Less(t, duracao, 15*time.Millisecond, "fail-fast deve responder em poucos milissegundos")
}

// -------------------------------------------------------------------------
// Validação 3, 4: Circuito HALF_OPEN e Recuperação para CLOSED
// -------------------------------------------------------------------------
func TestValidacao_3_4_HalfOpen_RecuperacaoParaClosed(t *testing.T) {
	var requisicoes atomic.Int64
	var servidorRecuperado atomic.Bool

	srv := servidorFalso(t, func(w http.ResponseWriter, _ *http.Request) {
		requisicoes.Add(1)
		if servidorRecuperado.Load() {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"display_name":"Rua Recuperada, Centro"}`))
			return
		}
		w.WriteHeader(http.StatusBadGateway) // HTTP 502
	})

	cli := cliente.NovoNominatimComOpcoes(cliente.OpcoesNominatim{
		URLBase:            srv.URL,
		Agente:             "validacao-cb-3-4",
		Timeout:            500 * time.Millisecond,
		MaxFalhasCB:        3,
		TimeoutRecuperacao: 40 * time.Millisecond,
		IntervaloMinimo:    0,
	})

	// Dispara 3 falhas para abrir o circuito
	for i := 0; i < 3; i++ {
		_, _ = cli.EnderecoAproximado(context.Background(), araranguaCentro)
	}
	assert.Equal(t, resiliencia.EstadoAberto, cli.CircuitBreaker().Estado())

	// Aguarda o término da janela de recuperação
	time.Sleep(50 * time.Millisecond)

	// O serviço externo se recupera
	servidorRecuperado.Store(true)

	// A próxima requisição é a requisição sonda (teste)
	end, err := cli.EnderecoAproximado(context.Background(), araranguaCentro)
	require.NoError(t, err)
	assert.Equal(t, "Rua Recuperada, Centro", end)
	// Comprovado: circuito restabelecido para CLOSED e falhas zeradas
	assert.Equal(t, resiliencia.EstadoFechado, cli.CircuitBreaker().Estado())
	assert.Equal(t, 0, cli.CircuitBreaker().FalhasConsecutivas())
}

// -------------------------------------------------------------------------
// Validação 5: Se continuar falhando, retorna para OPEN
// -------------------------------------------------------------------------
func TestValidacao_5_HalfOpen_SondaFalha_RetornaParaOpen(t *testing.T) {
	srv := servidorFalso(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})

	cli := cliente.NovoNominatimComOpcoes(cliente.OpcoesNominatim{
		URLBase:            srv.URL,
		Agente:             "validacao-cb-5",
		Timeout:            500 * time.Millisecond,
		MaxFalhasCB:        2,
		TimeoutRecuperacao: 35 * time.Millisecond,
		IntervaloMinimo:    0,
	})

	// Abre o circuito
	_, _ = cli.EnderecoAproximado(context.Background(), araranguaCentro)
	_, _ = cli.EnderecoAproximado(context.Background(), araranguaCentro)
	assert.Equal(t, resiliencia.EstadoAberto, cli.CircuitBreaker().Estado())

	// Aguarda janela de resfriamento
	time.Sleep(45 * time.Millisecond)

	// Sonda executa e falha (serviço ainda fora do ar)
	_, err := cli.EnderecoAproximado(context.Background(), araranguaCentro)
	assert.ErrorIs(t, err, cliente.ErrServicoIndisponivel)
	// Comprovado: circuito retorna para OPEN imediatamente
	assert.Equal(t, resiliencia.EstadoAberto, cli.CircuitBreaker().Estado())

	// Próxima chamada sofre fail-fast
	_, err = cli.EnderecoAproximado(context.Background(), araranguaCentro)
	assert.ErrorIs(t, err, cliente.ErrCircuitoAberto)
}

// -------------------------------------------------------------------------
// Validação 6: Alerta e Outbox salvos com coordenadas preservadas mesmo com Nominatim fora
// -------------------------------------------------------------------------
func TestValidacao_6_AlertaEOutboxPreservados_ComNominatimIndisponivel(t *testing.T) {
	dbPath := "file:" + filepath.Join(t.TempDir(), "test_validacao_6.db")
	repo, err := armazenamento.AbrirSQLite(context.Background(), dbPath)
	require.NoError(t, err)
	defer repo.Fechar()

	// Mock do Nominatim sempre fora do ar (503)
	srv := servidorFalso(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	})

	cli := cliente.NovoNominatimComOpcoes(cliente.OpcoesNominatim{
		URLBase:            srv.URL,
		Agente:             "validacao-cb-6",
		Timeout:            50 * time.Millisecond,
		MaxFalhasCB:        1,
		TimeoutRecuperacao: 10 * time.Second,
		IntervaloMinimo:    0,
	})

	desp := notificacao.NovoDespachante(notificacao.SMSNotificador{}, notificacao.EmailNotificador{})
	servico := alerta.NovoServico(repo, desp, cli, time.Second)

	notificouRelay := false
	servico.AtivarModoAssincrono(func() {
		notificouRelay = true
	})

	localEsperado := alerta.Localizacao{Latitude: -28.934600, Longitude: -49.495700}
	inicio := time.Now()

	a, err := servico.Acionar(context.Background(), alerta.PedidoAlerta{
		UsuariaID: armazenamento.UsuariaDemo,
		Nivel:     alerta.RiscoCritico,
		Local:     localEsperado,
		Mensagem:  "socorro teste validacao 6",
	})
	duracao := time.Since(inicio)

	// 1. Acionamento tem sucesso sem ser bloqueado pela API externa
	require.NoError(t, err)
	assert.Equal(t, alerta.StatusProcessando, a.Status)
	assert.True(t, notificouRelay)
	assert.Less(t, duracao, 50*time.Millisecond, "o tempo de resposta não pode ser impactado pela geocodificação")

	// 2. Coordenadas 100% preservadas
	assert.Equal(t, localEsperado.Latitude, a.Local.Latitude)
	assert.Equal(t, localEsperado.Longitude, a.Local.Longitude)

	// 3. Persistência no SQLite verificada
	salvo, err := repo.BuscarAlerta(context.Background(), a.ID)
	require.NoError(t, err)
	assert.Equal(t, localEsperado.Latitude, salvo.Local.Latitude)
	assert.Equal(t, localEsperado.Longitude, salvo.Local.Longitude)

	// 4. Tarefa Transactional Outbox gravada com sucesso
	tarefas, err := repo.BuscarTarefasPendentes(context.Background(), 10)
	require.NoError(t, err)
	require.Len(t, tarefas, 1)
	assert.Equal(t, a.ID, tarefas[0].AlertaID)
	assert.Equal(t, alerta.EstadoTarefaPendente, tarefas[0].Estado)
}

// -------------------------------------------------------------------------
// Validação 7: RabbitMQ e os Dois Consumidores continuam funcionando
// -------------------------------------------------------------------------
func TestValidacao_7_RabbitMQ_DoisConsumidores_ComCircuitBreaker(t *testing.T) {
	amqpURL := os.Getenv("AMQP_URL")
	if amqpURL == "" {
		amqpURL = "amqp://guest:guest@127.0.0.1:5672/"
	}

	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		t.Skip("RabbitMQ local indisponível, ignorando teste de mensageria real:", err)
		return
	}
	_ = conn.Close()

	dbPath := "file:" + filepath.Join(t.TempDir(), "test_validacao_7.db")
	repo, err := armazenamento.AbrirSQLite(context.Background(), dbPath)
	require.NoError(t, err)
	defer repo.Fechar()

	// Geocodificador com Circuit Breaker já aberto
	srv := servidorFalso(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	cli := cliente.NovoNominatimComOpcoes(cliente.OpcoesNominatim{
		URLBase:            srv.URL,
		Agente:             "validacao-cb-7",
		Timeout:            50 * time.Millisecond,
		MaxFalhasCB:        1,
		TimeoutRecuperacao: time.Hour,
		IntervaloMinimo:    0,
	})
	// Força abertura imediata
	_, _ = cli.EnderecoAproximado(context.Background(), araranguaCentro)
	require.Equal(t, resiliencia.EstadoAberto, cli.CircuitBreaker().Estado())

	desp := notificacao.NovoDespachante(notificacao.SMSNotificador{}, notificacao.EmailNotificador{})
	servico := alerta.NovoServico(repo, desp, cli, time.Second)

	opts := fila.OpcoesRabbitMQ{
		URL:                    amqpURL,
		ExchangeEventos:        "test.cb.eventos",
		ExchangeDLX:            "test.cb.dlx",
		FilaNotificacoes:       "test.cb.notificacoes",
		RoutingKeyNotificacoes: "alerta.criado",
		FilaAuditoria:          "test.cb.auditoria",
		RoutingKeyAuditoria:    "#",
		FilaDLQ:                "test.cb.dlq",
	}

	mgr, err := fila.NovoGerenciadorRabbitMQ(opts)
	require.NoError(t, err)
	defer mgr.Fechar()

	relay := fila.NovoOutboxRelay(repo, mgr, "alerta.criado", 50*time.Millisecond)
	relay.Iniciar()
	defer relay.Parar()

	servico.AtivarModoAssincrono(relay.NotificarNovoAlerta)

	// Consumidor A: Notificações
	consA, err := fila.NovoConsumidorNotificacoes(mgr.Conexao(), servico, repo, fila.OpcoesConsumidorNotificacoes{
		Fila: opts.FilaNotificacoes,
	})
	require.NoError(t, err)
	require.NoError(t, consA.Iniciar())
	defer consA.Parar(time.Second)

	// Consumidor B: Auditoria
	consB, err := fila.NovoConsumidorAuditoria(mgr.Conexao(), servico, fila.OpcoesConsumidorAuditoria{
		Fila: opts.FilaAuditoria,
	})
	require.NoError(t, err)
	require.NoError(t, consB.Iniciar())
	defer consB.Parar(time.Second)

	// Aciona o alerta de emergência
	alertaCriado, err := servico.Acionar(context.Background(), alerta.PedidoAlerta{
		UsuariaID: armazenamento.UsuariaDemo,
		Nivel:     alerta.RiscoCritico,
		Local:     araranguaCentro,
		Mensagem:  "socorro teste validacao 7",
	})
	require.NoError(t, err)
	assert.Equal(t, alerta.StatusProcessando, alertaCriado.Status)

	// 1. Comprova que o Consumidor A processou o alerta e atualizou para CONCLUIDO no SQLite
	require.Eventually(t, func() bool {
		alt, err := repo.BuscarAlerta(context.Background(), alertaCriado.ID)
		return err == nil && alt.Status == alerta.StatusConcluido
	}, 3*time.Second, 100*time.Millisecond)

	// 2. Comprova que o Consumidor B auditou o evento na tabela de auditoria
	require.Eventually(t, func() bool {
		audit, err := repo.ListarAuditoria(context.Background(), 10)
		if err != nil {
			return false
		}
		for _, reg := range audit {
			if reg.AlertaID == alertaCriado.ID {
				return true
			}
		}
		return false
	}, 3*time.Second, 100*time.Millisecond)
}

// -------------------------------------------------------------------------
// Validação 8: Limite de Requisições do Nominatim é respeitado (Rate Limiting)
// -------------------------------------------------------------------------
func TestValidacao_8_RateLimiting_RespeitaIntervalo(t *testing.T) {
	srv := servidorFalso(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"display_name":"Rua Teste Rate Limiting"}`))
	})

	intervalo := 30 * time.Millisecond
	cli := cliente.NovoNominatimComOpcoes(cliente.OpcoesNominatim{
		URLBase:            srv.URL,
		Agente:             "validacao-cb-8",
		Timeout:            time.Second,
		MaxFalhasCB:        3,
		TimeoutRecuperacao: time.Second,
		IntervaloMinimo:    intervalo,
	})

	inicio := time.Now()
	_, err1 := cli.EnderecoAproximado(context.Background(), araranguaCentro)
	require.NoError(t, err1)

	// Segunda chamada consecutiva: deve aguardar o intervalo do rate limiter
	_, err2 := cli.EnderecoAproximado(context.Background(), araranguaCentro)
	require.NoError(t, err2)

	duracao := time.Since(inicio)
	assert.GreaterOrEqual(t, duracao, 25*time.Millisecond, "o rate limiter deve impor espaçamento mínimo entre requisições")
}
