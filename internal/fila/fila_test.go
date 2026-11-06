package fila_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/plataforma-mulher/botao-panico/internal/alerta"
	"github.com/plataforma-mulher/botao-panico/internal/armazenamento"
	"github.com/plataforma-mulher/botao-panico/internal/fila"
	"github.com/plataforma-mulher/botao-panico/internal/httpapi"
	amqp "github.com/rabbitmq/amqp091-go"
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

// publicadorMock implementa fila.Publicador para testes unitários isolados
type publicadorMock struct {
	mu           sync.Mutex
	publicados   []alerta.Envelope
	routingKeys  []string
	falhaForcada error
}

func (p *publicadorMock) Publicar(ctx context.Context, routingKey string, env alerta.Envelope) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.falhaForcada != nil {
		return p.falhaForcada
	}
	p.publicados = append(p.publicados, env)
	p.routingKeys = append(p.routingKeys, routingKey)
	return nil
}

// 1. Gravação atômica do alerta e da outbox (Transactional Outbox)
func TestFila_GravacaoAtomicaAlertaEOutbox(t *testing.T) {
	dbPath := "file:" + filepath.Join(t.TempDir(), "test_outbox.db")
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
		Topico:             "alerta.criado",
		PayloadJSON:        `{"evento":"alerta.criado","versao":1,"id_evento":"evt-1","dados":{"tarefa_id":"tar-atomica-1","alerta_id":"alt-atomico-1"}}`,
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
}

// 2. Outbox Relay publica tarefas com publisher confirms e atualiza status no SQLite
func TestFila_OutboxRelay_PublicacaoComConfirmacao(t *testing.T) {
	dbPath := "file:" + filepath.Join(t.TempDir(), "test_relay.db")
	repo, err := armazenamento.AbrirSQLite(context.Background(), dbPath)
	require.NoError(t, err)
	defer repo.Fechar()

	pub := &publicadorMock{}
	relay := fila.NovoOutboxRelay(repo, pub, "alerta.criado", 50*time.Millisecond)

	agora := time.Now().UTC()
	env := alerta.Envelope{
		Evento:    "alerta.criado",
		Versao:    1,
		IDEvento:  "evt-test-101",
		EmitidoEm: agora,
		Dados: alerta.DadosEvento{
			TarefaID:  "tar-relay-101",
			AlertaID:  "alt-relay-101",
			UsuariaID: "usuaria-demo",
			Nivel:     "alto",
		},
	}
	payloadBytes, _ := json.Marshal(env)

	tarefa := alerta.TarefaOutbox{
		ID:                 "tar-relay-101",
		AlertaID:           "alt-relay-101",
		Topico:             "alerta.criado",
		PayloadJSON:        string(payloadBytes),
		Estado:             alerta.EstadoTarefaPendente,
		MaxTentativas:      5,
		ProximaTentativaEm: agora,
		CriadoEm:           agora,
		AtualizadoEm:       agora,
	}
	alt := alerta.Alerta{ID: "alt-relay-101", UsuariaID: "usuaria-demo", Nivel: alerta.RiscoAlto, Status: alerta.StatusProcessando, CriadoEm: agora}

	require.NoError(t, repo.SalvarAlertaComOutbox(context.Background(), alt, tarefa))

	relay.Iniciar()
	defer relay.Parar()

	relay.NotificarNovoAlerta()

	// Aguarda processamento pelo Relay
	require.Eventually(t, func() bool {
		pub.mu.Lock()
		defer pub.mu.Unlock()
		return len(pub.publicados) == 1
	}, 2*time.Second, 50*time.Millisecond)

	// Verifica status atualizado no SQLite
	tarAtualizada, err := repo.BuscarTarefaPorID(context.Background(), "tar-relay-101")
	require.NoError(t, err)
	assert.Equal(t, alerta.EstadoTarefaPublicado, tarAtualizada.Estado)
	assert.Equal(t, "alerta.criado", pub.routingKeys[0])
	assert.Equal(t, "evt-test-101", pub.publicados[0].IDEvento)
}

// 3. Recuperação do Outbox Relay após indisponibilidade temporária do broker
func TestFila_OutboxRelay_RecuperacaoAposIndisponibilidade(t *testing.T) {
	dbPath := "file:" + filepath.Join(t.TempDir(), "test_resiliencia.db")
	repo, err := armazenamento.AbrirSQLite(context.Background(), dbPath)
	require.NoError(t, err)
	defer repo.Fechar()

	pub := &publicadorMock{falhaForcada: errors.New("conexão amqp recusada")}
	relay := fila.NovoOutboxRelay(repo, pub, "alerta.criado", 50*time.Millisecond)

	agora := time.Now().UTC()
	env := alerta.Envelope{
		Evento:    "alerta.criado",
		Versao:    1,
		IDEvento:  "evt-res-202",
		EmitidoEm: agora,
		Dados: alerta.DadosEvento{
			TarefaID: "tar-res-202",
			AlertaID: "alt-res-202",
		},
	}
	payloadBytes, _ := json.Marshal(env)

	tarefa := alerta.TarefaOutbox{
		ID:                 "tar-res-202",
		AlertaID:           "alt-res-202",
		Topico:             "alerta.criado",
		PayloadJSON:        string(payloadBytes),
		Estado:             alerta.EstadoTarefaPendente,
		MaxTentativas:      5,
		ProximaTentativaEm: agora,
		CriadoEm:           agora,
		AtualizadoEm:       agora,
	}
	alt := alerta.Alerta{ID: "alt-res-202", UsuariaID: "usuaria-demo", Nivel: alerta.RiscoAlto, Status: alerta.StatusProcessando, CriadoEm: agora}

	require.NoError(t, repo.SalvarAlertaComOutbox(context.Background(), alt, tarefa))

	relay.Iniciar()

	// Tentativa falha -> tarefa deve permanecer pendente com tentativas incrementadas
	time.Sleep(150 * time.Millisecond)
	tar, err := repo.BuscarTarefaPorID(context.Background(), "tar-res-202")
	require.NoError(t, err)
	assert.Equal(t, alerta.EstadoTarefaPendente, tar.Estado)
	assert.GreaterOrEqual(t, tar.Tentativas, 1)

	// Broker se recupera
	pub.mu.Lock()
	pub.falhaForcada = nil
	pub.mu.Unlock()

	// Ajusta proxima tentativa para agora para simular passar do tempo de backoff
	_ = repo.IncrementarFalhaTarefa(context.Background(), "tar-res-202", "erro anterior", time.Now().Add(-time.Second))
	relay.NotificarNovoAlerta()

	require.Eventually(t, func() bool {
		tAtual, err := repo.BuscarTarefaPorID(context.Background(), "tar-res-202")
		return err == nil && tAtual.Estado == alerta.EstadoTarefaPublicado
	}, 2*time.Second, 50*time.Millisecond)

	relay.Parar()
}

// 4. Idempotência no Consumidor contra mensagens repetidas
func TestFila_IdempotenciaConsumidor(t *testing.T) {
	dbPath := "file:" + filepath.Join(t.TempDir(), "test_idempotencia.db")
	repo, err := armazenamento.AbrirSQLite(context.Background(), dbPath)
	require.NoError(t, err)
	defer repo.Fechar()

	mockDesp := &despachanteMock{}
	servico := alerta.NovoServico(repo, mockDesp, nil, time.Second)

	agora := time.Now().UTC()
	alt := alerta.Alerta{
		ID:        "alt-idemp-1",
		UsuariaID: armazenamento.UsuariaDemo,
		Nivel:     alerta.RiscoAlto,
		Status:    alerta.StatusProcessando,
		CriadoEm:  agora,
	}
	tarefa := alerta.TarefaOutbox{
		ID:           "tar-idemp-1",
		AlertaID:     "alt-idemp-1",
		Topico:       "alerta.criado",
		PayloadJSON:  `{}`,
		Estado:       alerta.EstadoTarefaPublicado,
		CriadoEm:     agora,
		AtualizadoEm: agora,
	}
	require.NoError(t, repo.SalvarAlertaComOutbox(context.Background(), alt, tarefa))

	// Primeira execução: processa as notificações
	err = servico.ProcessarTarefaNotificacao(context.Background(), "alt-idemp-1")
	require.NoError(t, err)
	assert.Equal(t, int64(1), atomic.LoadInt64(&mockDesp.enviosTotais))

	altFinal, err := repo.BuscarAlerta(context.Background(), "alt-idemp-1")
	require.NoError(t, err)
	assert.Equal(t, alerta.StatusConcluido, altFinal.Status)

	// Segunda execução da mesma mensagem (replay do broker): não redespacha notificações
	err = servico.ProcessarTarefaNotificacao(context.Background(), "alt-idemp-1")
	require.NoError(t, err)
	assert.Equal(t, int64(1), atomic.LoadInt64(&mockDesp.enviosTotais), "não deve duplicar notificações")
}

// 5. Concorrência entre workers disputando tarefas distintas
func TestFila_ConcorrenciaWorkers(t *testing.T) {
	dbPath := "file:" + filepath.Join(t.TempDir(), "test_concorrencia.db")
	repo, err := armazenamento.AbrirSQLite(context.Background(), dbPath)
	require.NoError(t, err)
	defer repo.Fechar()

	mockDesp := &despachanteMock{}
	servico := alerta.NovoServico(repo, mockDesp, nil, 2*time.Second)

	totalAlertas := 10
	var wg sync.WaitGroup
	wg.Add(totalAlertas)

	for i := 0; i < totalAlertas; i++ {
		alertaID := fmt.Sprintf("alt-conc-%d", i)
		alt := alerta.Alerta{
			ID:        alertaID,
			UsuariaID: armazenamento.UsuariaDemo,
			Nivel:     alerta.RiscoAlto,
			Status:    alerta.StatusProcessando,
			CriadoEm:  time.Now().UTC(),
		}
		tarefa := alerta.TarefaOutbox{
			ID:       "tar-conc-" + alertaID,
			AlertaID: alertaID,
			Topico:   "alerta.criado",
			Estado:   alerta.EstadoTarefaPendente,
		}
		require.NoError(t, repo.SalvarAlertaComOutbox(context.Background(), alt, tarefa))

		go func(id string) {
			defer wg.Done()
			_ = servico.ProcessarTarefaNotificacao(context.Background(), id)
		}(alertaID)
	}

	wg.Wait()
	assert.Equal(t, int64(totalAlertas), atomic.LoadInt64(&mockDesp.enviosTotais))
}

// 6. Persistência de Auditoria no SQLite
func TestFila_AuditoriaEventosSQLite(t *testing.T) {
	dbPath := "file:" + filepath.Join(t.TempDir(), "test_auditoria.db")
	repo, err := armazenamento.AbrirSQLite(context.Background(), dbPath)
	require.NoError(t, err)
	defer repo.Fechar()

	servico := alerta.NovoServico(repo, &despachanteMock{}, nil, time.Second)

	agora := time.Now().UTC()
	reg := alerta.RegistroAuditoria{
		ID:         "aud-001",
		Evento:     "alerta.criado",
		Versao:     1,
		IDEvento:   "evt-999",
		AlertaID:   "alt-999",
		RoutingKey: "alerta.criado",
		RecebidoEm: agora,
	}

	err = servico.SalvarAuditoria(context.Background(), reg)
	require.NoError(t, err)

	registros, err := servico.ListarAuditoria(context.Background(), 10)
	require.NoError(t, err)
	require.Len(t, registros, 1)
	assert.Equal(t, "aud-001", registros[0].ID)
	assert.Equal(t, "alerta.criado", registros[0].Evento)
	assert.Equal(t, "alt-999", registros[0].AlertaID)
}

// 7. Integração REST -> SQLite com Modo Assíncrono ativado
func TestFila_IntegracaoRESTComModoAssincrono(t *testing.T) {
	dbPath := "file:" + filepath.Join(t.TempDir(), "test_http_outbox.db")
	repo, err := armazenamento.AbrirSQLite(context.Background(), dbPath)
	require.NoError(t, err)
	defer repo.Fechar()

	servico := alerta.NovoServico(repo, &despachanteMock{}, nil, time.Second)

	notificouRelay := false
	servico.AtivarModoAssincrono(func() {
		notificouRelay = true
	})

	srv := httpapi.NovoServidor(servico)
	handler := srv.Rotas()

	corpo := `{"usuaria_id":"usuaria-demo","nivel":"alto","mensagem":"perigo","local":{"latitude":-28.93,"longitude":-49.49}}`
	req := httptest.NewRequest("POST", "/alertas", strings.NewReader(corpo))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusCreated, rec.Code)
	assert.True(t, notificouRelay, "deve notificar o outbox relay imediatamente")

	var resposta map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resposta))
	assert.Equal(t, "processando", resposta["status"])
	assert.NotEmpty(t, resposta["id"])
}

// 8. Teste de Integração Real com RabbitMQ (executa somente se broker estiver acessível)
func TestFila_RabbitMQIntegracao(t *testing.T) {
	amqpURL := os.Getenv("AMQP_URL")
	if amqpURL == "" {
		amqpURL = "amqp://guest:guest@127.0.0.1:5672/"
	}

	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		t.Skip("RabbitMQ local não está acessível no momento, ignorando teste de integração real:", err)
		return
	}
	_ = conn.Close()

	opts := fila.OpcoesRabbitMQ{
		URL:                    amqpURL,
		ExchangeEventos:        "test.panico.eventos",
		ExchangeDLX:            "test.panico.dlx",
		FilaNotificacoes:       "test.panico.notificacoes",
		RoutingKeyNotificacoes: "alerta.criado",
		FilaAuditoria:          "test.panico.auditoria",
		RoutingKeyAuditoria:    "#",
		FilaDLQ:                "test.panico.dlq",
	}

	mgr, err := fila.NovoGerenciadorRabbitMQ(opts)
	require.NoError(t, err)
	defer mgr.Fechar()

	// Publica um evento de teste com Publisher Confirms
	agora := time.Now().UTC()
	env := alerta.Envelope{
		Evento:    "alerta.criado",
		Versao:    1,
		IDEvento:  "evt-rabbitmq-test",
		EmitidoEm: agora,
		Dados: alerta.DadosEvento{
			TarefaID:  "tar-rabbitmq-test",
			AlertaID:  "alt-rabbitmq-test",
			UsuariaID: "usuaria-demo",
			Nivel:     "alto",
		},
	}

	err = mgr.Publicar(context.Background(), "alerta.criado", env)
	require.NoError(t, err, "publicação no rabbitmq com publisher confirm deve suceder")
}
