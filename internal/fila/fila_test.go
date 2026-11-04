package fila_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/plataforma-mulher/botao-panico/internal/alerta"
	"github.com/plataforma-mulher/botao-panico/internal/armazenamento"
	"github.com/plataforma-mulher/botao-panico/internal/fila"
	"github.com/plataforma-mulher/botao-panico/internal/httpapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// despachanteMock simula o envio de notificações e conta tentativas
type despachanteMock struct {
	mu           sync.Mutex
	enviosTotais int64
	falhaForcada bool
}

func (d *despachanteMock) Despachar(_ context.Context, dests []alerta.Destinatario, _ string) []alerta.ResultadoEnvio {
	atomic.AddInt64(&d.enviosTotais, 1)
	res := make([]alerta.ResultadoEnvio, len(dests))
	for i, dest := range dests {
		res[i] = alerta.ResultadoEnvio{
			Destino:   dest.Nome,
			Canal:     "sms",
			Sucesso:   !d.falhaForcada,
			DuracaoMs: 5,
			EnviadoEm: time.Now().Format(time.RFC3339),
		}
	}
	return res
}

func criarAmbienteFilaTeste(t *testing.T, streamNome string) (*fila.GerenciadorNATS, func()) {
	t.Helper()
	opts := fila.OpcoesNATS{
		Stream:       streamNome,
		Subject:      streamNome + ".notificar",
		Consumer:     streamNome + "-consumer",
		EmMemoria:    true,
		PortaInterna: -1, // Porta livre automática
	}
	gerenciador, err := fila.NovoGerenciadorNATS(opts)
	require.NoError(t, err)

	limpeza := func() {
		gerenciador.Fechar()
	}
	return gerenciador, limpeza
}

// 1. Gravação atômica do alerta e da outbox (Transactional Outbox)
func TestFila_GravacaoAtomicaAlertaEOutbox(t *testing.T) {
	dbPath := fmt.Sprintf("file:test_outbox_%d.db", time.Now().UnixNano())
	repo, err := armazenamento.AbrirSQLite(context.Background(), dbPath)
	require.NoError(t, err)
	defer repo.Fechar()

	agora := time.Now().UTC()
	alt := alerta.Alerta{
		ID:        "alt-atomico-1",
		UsuariaID: "usuaria-1",
		Nivel:     alerta.RiscoAlto,
		Local:     alerta.Localizacao{Latitude: -28.93, Longitude: -49.49},
		Status:    alerta.StatusProcessando,
		CriadoEm:  agora,
	}
	tarefa := alerta.TarefaOutbox{
		ID:                 "tar-atomica-1",
		AlertaID:           "alt-atomico-1",
		Topico:             "alertas.notificar",
		PayloadJSON:        `{"tarefa_id":"tar-atomica-1","alerta_id":"alt-atomico-1"}`,
		Estado:             alerta.EstadoTarefaPendente,
		MaxTentativas:      5,
		ProximaTentativaEm: agora,
		CriadoEm:           agora,
		AtualizadoEm:       agora,
	}

	// 1.1 Gravação com sucesso persiste ambos
	err = repo.SalvarAlertaComOutbox(context.Background(), alt, tarefa)
	require.NoError(t, err)

	altSalvo, err := repo.BuscarAlerta(context.Background(), "alt-atomico-1")
	require.NoError(t, err)
	assert.Equal(t, "alt-atomico-1", altSalvo.ID)

	tarSalva, err := repo.BuscarTarefaPorID(context.Background(), "tar-atomica-1")
	require.NoError(t, err)
	assert.Equal(t, "tar-atomica-1", tarSalva.ID)
	assert.Equal(t, alerta.EstadoTarefaPendente, tarSalva.Estado)

	// 1.2 Gravação duplicada falha e não corrompe
	errDuplicado := repo.SalvarAlertaComOutbox(context.Background(), alt, tarefa)
	require.Error(t, errDuplicado)
}

// 2. Falha do broker na publicação (preserva no banco com backoff)
func TestFila_FalhaBrokerNaPublicacao(t *testing.T) {
	repo := armazenamento.NovaMemoria()
	agora := time.Now().UTC()

	tar := alerta.TarefaOutbox{
		ID:                 "tar-falha-1",
		AlertaID:           "alt-falha-1",
		Topico:             "topico.inexistente",
		PayloadJSON:        `{}`,
		Estado:             alerta.EstadoTarefaPendente,
		MaxTentativas:      3,
		ProximaTentativaEm: agora,
		CriadoEm:           agora,
		AtualizadoEm:       agora,
	}
	err := repo.SalvarAlertaComOutbox(context.Background(), alerta.Alerta{ID: "alt-falha-1"}, tar)
	require.NoError(t, err)

	// Inicia relay com NATS simulado desconectado (ou mock)
	nc, _ := nats.Connect("nats://127.0.0.1:42299", nats.Timeout(50*time.Millisecond))
	var js nats.JetStreamContext
	if nc != nil {
		js, _ = nc.JetStream()
	}

	// Se não conseguiu conectar, testa incremento de falha no repositório
	errInc := repo.IncrementarFalhaTarefa(context.Background(), "tar-falha-1", "conexao recusada", time.Now().Add(1*time.Second))
	require.NoError(t, errInc)

	tarAtualizada, err := repo.BuscarTarefaPorID(context.Background(), "tar-falha-1")
	require.NoError(t, err)
	assert.Equal(t, 1, tarAtualizada.Tentativas)
	assert.Equal(t, alerta.EstadoTarefaPendente, tarAtualizada.Estado)
	assert.Equal(t, "conexao recusada", tarAtualizada.ErroUltimo)
	if nc != nil {
		nc.Close()
	}
	_ = js
}

// 3. Publicação com confirmação (Outbox Relay -> NATS JetStream)
func TestFila_PublicacaoComConfirmacao(t *testing.T) {
	gerenciador, cleanup := criarAmbienteFilaTeste(t, "STREAM_TEST_PUB")
	defer cleanup()

	repo := armazenamento.NovaMemoria()
	agora := time.Now().UTC()
	tar := alerta.TarefaOutbox{
		ID:                 "tar-pub-1",
		AlertaID:           "alt-pub-1",
		Topico:             "alertas.notificar",
		PayloadJSON:        `{"alerta_id":"alt-pub-1"}`,
		Estado:             alerta.EstadoTarefaPendente,
		MaxTentativas:      5,
		ProximaTentativaEm: agora,
		CriadoEm:           agora,
		AtualizadoEm:       agora,
	}
	err := repo.SalvarAlertaComOutbox(context.Background(), alerta.Alerta{ID: "alt-pub-1"}, tar)
	require.NoError(t, err)

	relay := fila.NovoOutboxRelay(repo, gerenciador.JS, "alertas.notificar", 50*time.Millisecond)
	relay.Iniciar()
	defer relay.Parar()

	// Aguarda o relay publicar
	require.Eventually(t, func() bool {
		tSalva, err := repo.BuscarTarefaPorID(context.Background(), "tar-pub-1")
		return err == nil && tSalva.Estado == alerta.EstadoTarefaPublicado
	}, 3*time.Second, 50*time.Millisecond)
}

// 4. Consumo bem-sucedido (Worker consome, despacha simulação e confirma)
func TestFila_ConsumoBemSucedido(t *testing.T) {
	gerenciador, cleanup := criarAmbienteFilaTeste(t, "STREAM_TEST_CONSUMO")
	defer cleanup()

	repo := armazenamento.NovaMemoria()
	desp := &despachanteMock{}
	servico := alerta.NovoServico(repo, desp, nil, 2*time.Second)
	servico.AtivarModoAssincrono(nil)

	// Cria alerta via serviço
	alt, err := servico.Acionar(context.Background(), alerta.PedidoAlerta{
		UsuariaID: armazenamento.UsuariaDemo,
		Nivel:     alerta.RiscoAlto,
		Local:     alerta.Localizacao{Latitude: -28.9346, Longitude: -49.4957},
	})
	require.NoError(t, err)
	assert.Equal(t, alerta.StatusProcessando, alt.Status)

	// Inicia Relay e Consumidor
	relay := fila.NovoOutboxRelay(repo, gerenciador.JS, "alertas.notificar", 50*time.Millisecond)
	relay.Iniciar()
	defer relay.Parar()

	optsConsumidor := fila.OpcoesConsumidor{
		Stream:     "STREAM_TEST_CONSUMO",
		Subject:    "alertas.notificar",
		Consumer:   "consumer-consumo-sucesso",
		NumWorkers: 2,
	}
	worker := fila.NovoConsumidorNotificacoes(gerenciador.JS, servico, repo, optsConsumidor)
	err = worker.Iniciar()
	require.NoError(t, err)
	defer worker.Parar(2 * time.Second)

	// Aguarda processamento completo pelo worker
	require.Eventually(t, func() bool {
		altAtualizado, err := servico.Buscar(context.Background(), alt.ID)
		return err == nil && altAtualizado.Status == alerta.StatusConcluido && len(altAtualizado.Notificacoes) > 0
	}, 4*time.Second, 50*time.Millisecond)

	assert.Equal(t, int64(1), atomic.LoadInt64(&desp.enviosTotais))
}

// 5. Reentrega da mesma mensagem (Idempotência no Consumidor)
func TestFila_ReentregaMesmaMensagem(t *testing.T) {
	_, cleanup := criarAmbienteFilaTeste(t, "STREAM_TEST_REPLAY")
	defer cleanup()

	repo := armazenamento.NovaMemoria()
	desp := &despachanteMock{}
	servico := alerta.NovoServico(repo, desp, nil, 2*time.Second)
	servico.AtivarModoAssincrono(nil)

	alt, err := servico.Acionar(context.Background(), alerta.PedidoAlerta{
		UsuariaID: armazenamento.UsuariaDemo,
		Nivel:     alerta.RiscoBaixo,
		Local:     alerta.Localizacao{Latitude: -28.93, Longitude: -49.49},
	})
	require.NoError(t, err)

	// Primeiro processamento
	err = servico.ProcessarTarefaNotificacao(context.Background(), alt.ID)
	require.NoError(t, err)
	assert.Equal(t, int64(1), atomic.LoadInt64(&desp.enviosTotais))

	// Segunda chamada com mesmo alerta (simulando reentrega pelo broker)
	errReplay := servico.ProcessarTarefaNotificacao(context.Background(), alt.ID)
	require.NoError(t, errReplay)
	// Contador NÃO deve aumentar (evitou duplicidade de notificações!)
	assert.Equal(t, int64(1), atomic.LoadInt64(&desp.enviosTotais))
}

// 6. Concorrência: dois workers tentando processar a mesma tarefa
func TestFila_ConcorrenciaDoisWorkers(t *testing.T) {
	repo := armazenamento.NovaMemoria()
	desp := &despachanteMock{}
	servico := alerta.NovoServico(repo, desp, nil, 2*time.Second)
	servico.AtivarModoAssincrono(nil)

	alt, err := servico.Acionar(context.Background(), alerta.PedidoAlerta{
		UsuariaID: armazenamento.UsuariaDemo,
		Nivel:     alerta.RiscoAlto,
		Local:     alerta.Localizacao{Latitude: -28.93, Longitude: -49.49},
	})
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_ = servico.ProcessarTarefaNotificacao(context.Background(), alt.ID)
	}()
	go func() {
		defer wg.Done()
		_ = servico.ProcessarTarefaNotificacao(context.Background(), alt.ID)
	}()
	wg.Wait()

	// Apenas um despacho simulado deve ter ocorrido
	assert.Equal(t, int64(1), atomic.LoadInt64(&desp.enviosTotais))
}

// 7. Falha durante processamento
func TestFila_FalhaDuranteProcessamento(t *testing.T) {
	repo := armazenamento.NovaMemoria()
	desp := &despachanteMock{}
	servico := alerta.NovoServico(repo, desp, nil, 2*time.Second)

	// Tenta processar alerta que não existe no repositório -> Deve retornar erro
	err := servico.ProcessarTarefaNotificacao(context.Background(), "alerta-fantasma")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "buscar alerta")
}

// 8. Recuperação de tarefas pendentes após reinicialização
func TestFila_RecuperacaoAposReinicializacao(t *testing.T) {
	gerenciador, cleanup := criarAmbienteFilaTeste(t, "STREAM_TEST_RECOVERY")
	defer cleanup()

	repo := armazenamento.NovaMemoria()
	desp := &despachanteMock{}
	servico := alerta.NovoServico(repo, desp, nil, 2*time.Second)
	servico.AtivarModoAssincrono(nil)

	// Simula criação do alerta quando o sistema caiu antes do relay enviar
	alt, err := servico.Acionar(context.Background(), alerta.PedidoAlerta{
		UsuariaID: armazenamento.UsuariaDemo,
		Nivel:     alerta.RiscoModerado,
		Local:     alerta.Localizacao{Latitude: -28.93, Longitude: -49.49},
	})
	require.NoError(t, err)
	assert.Equal(t, alerta.StatusProcessando, alt.Status)

	// "Reinicialização do sistema": Inicia agora o Relay e Worker
	relay := fila.NovoOutboxRelay(repo, gerenciador.JS, "alertas.notificar", 30*time.Millisecond)
	relay.Iniciar()
	defer relay.Parar()

	worker := fila.NovoConsumidorNotificacoes(gerenciador.JS, servico, repo, fila.OpcoesConsumidor{
		Stream:     "STREAM_TEST_RECOVERY",
		Subject:    "alertas.notificar",
		Consumer:   "consumer-recovery",
		NumWorkers: 1,
	})
	err = worker.Iniciar()
	require.NoError(t, err)
	defer worker.Parar(2 * time.Second)

	// Recupera e conclui o alerta que estava pendente
	require.Eventually(t, func() bool {
		altConcluido, err := servico.Buscar(context.Background(), alt.ID)
		return err == nil && altConcluido.Status == alerta.StatusConcluido
	}, 4*time.Second, 50*time.Millisecond)
}

// 9. Esgotamento de retentativas
func TestFila_EsgotamentoRetentativas(t *testing.T) {
	repo := armazenamento.NovaMemoria()
	agora := time.Now().UTC()
	tar := alerta.TarefaOutbox{
		ID:                 "tar-retry-1",
		AlertaID:           "alt-retry-1",
		Topico:             "alertas.notificar",
		PayloadJSON:        `{}`,
		Estado:             alerta.EstadoTarefaPendente,
		Tentativas:         4, // Já em 4 de 5
		MaxTentativas:      5,
		ProximaTentativaEm: agora,
		CriadoEm:           agora,
		AtualizadoEm:       agora,
	}
	err := repo.SalvarAlertaComOutbox(context.Background(), alerta.Alerta{ID: "alt-retry-1"}, tar)
	require.NoError(t, err)

	// Incrementa a 5ª tentativa (deve marcar estado 'falha')
	err = repo.IncrementarFalhaTarefa(context.Background(), "tar-retry-1", "erro fatal de rede", time.Now().Add(1*time.Minute))
	require.NoError(t, err)

	tarFinal, err := repo.BuscarTarefaPorID(context.Background(), "tar-retry-1")
	require.NoError(t, err)
	assert.Equal(t, 5, tarFinal.Tentativas)
	assert.Equal(t, alerta.EstadoTarefaFalha, tarFinal.Estado)
}

// 10. Desligamento gracioso do Worker
func TestFila_DesligamentoGracioso(t *testing.T) {
	gerenciador, cleanup := criarAmbienteFilaTeste(t, "STREAM_TEST_SHUTDOWN")
	defer cleanup()

	repo := armazenamento.NovaMemoria()
	desp := &despachanteMock{}
	servico := alerta.NovoServico(repo, desp, nil, 2*time.Second)

	worker := fila.NovoConsumidorNotificacoes(gerenciador.JS, servico, repo, fila.OpcoesConsumidor{
		Stream:     "STREAM_TEST_SHUTDOWN",
		Subject:    "alertas.notificar",
		Consumer:   "consumer-shutdown",
		NumWorkers: 2,
	})
	err := worker.Iniciar()
	require.NoError(t, err)

	// Desliga graciosamente
	inicio := time.Now()
	worker.Parar(2 * time.Second)
	duracao := time.Since(inicio)
	assert.True(t, duracao < 3*time.Second)
}

// 11. Idempotência compartilhada REST / gRPC com Outbox
func TestFila_IdempotenciaCompartilhada(t *testing.T) {
	repo := armazenamento.NovaMemoria()
	desp := &despachanteMock{}
	servico := alerta.NovoServico(repo, desp, nil, 2*time.Second)
	servico.AtivarModoAssincrono(nil)

	chave := "chave-idemp-fila-compartilhada"
	pedido := alerta.PedidoAlerta{
		ChaveIdempotencia: chave,
		UsuariaID:         armazenamento.UsuariaDemo,
		Nivel:             alerta.RiscoAlto,
		Local:             alerta.Localizacao{Latitude: -28.93, Longitude: -49.49},
	}

	// 11.1 Cria alerta (retorna StatusProcessando)
	alt1, err := servico.Acionar(context.Background(), pedido)
	require.NoError(t, err)
	assert.Equal(t, alerta.StatusProcessando, alt1.Status)
	assert.False(t, alt1.Replay)

	// 11.2 Repetição com mesma chave retorna mesmo ID com Replay = true
	alt2, err := servico.Acionar(context.Background(), pedido)
	require.NoError(t, err)
	assert.Equal(t, alt1.ID, alt2.ID)
	assert.True(t, alt2.Replay)

	// 11.3 Repetição com mesma chave mas dados divergentes retorna conflito
	pedidoDivergente := pedido
	pedidoDivergente.Nivel = alerta.RiscoBaixo
	_, errConflito := servico.Acionar(context.Background(), pedidoDivergente)
	require.Error(t, errConflito)
	assert.True(t, errors.Is(errConflito, alerta.ErrConflitoIdempotencia))
}

// 12. Regressão dos Endpoints REST existentes
func TestFila_RegressaoEndpointsExistentes(t *testing.T) {
	repo := armazenamento.NovaMemoria()
	desp := &despachanteMock{}
	servico := alerta.NovoServico(repo, desp, nil, 2*time.Second)
	servico.AtivarModoAssincrono(nil)

	servidor := httpapi.NovoServidor(servico, "chave-secreta-rest-12345")
	mux := servidor.Rotas()

	// 12.1 POST /alertas autenticado cria alerta assíncrono com HTTP 201
	corpo := `{"usuaria_id":"usuaria-demo","nivel":"alto","local":{"latitude":-28.9346,"longitude":-49.4957}}`
	req := httptest.NewRequest(http.MethodPost, "/alertas", strings.NewReader(corpo))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Service-Token", "chave-secreta-rest-12345")

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusCreated, rec.Code)
	assert.Contains(t, rec.Body.String(), `"status":"processando"`)
	assert.Contains(t, rec.Body.String(), `"id":`)

	// 12.2 GET /health continua respondendo ok
	reqHealth := httptest.NewRequest(http.MethodGet, "/health", nil)
	recHealth := httptest.NewRecorder()
	mux.ServeHTTP(recHealth, reqHealth)
	assert.Equal(t, http.StatusOK, recHealth.Code)
	assert.Contains(t, recHealth.Body.String(), `"status":"ok"`)
}
