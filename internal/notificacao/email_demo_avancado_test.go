package notificacao_test

import (
	"context"
	"errors"
	"fmt"
	"net/smtp"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/plataforma-mulher/botao-panico/internal/alerta"
	"github.com/plataforma-mulher/botao-panico/internal/armazenamento"
	"github.com/plataforma-mulher/botao-panico/internal/notificacao"
	"github.com/plataforma-mulher/botao-panico/internal/observabilidade"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockSMTPSenderAvancado permite configurar comportamentos dinâmicos para testes de resiliência.
type mockSMTPSenderAvancado struct {
	mu            sync.Mutex
	totalEnvios   int
	destinatarios []string
	ultimoCorpo   []byte
	ultimoFrom    string
	delayEnvio    time.Duration
	erroDinamico  func(tentativa int) error
}

func (m *mockSMTPSenderAvancado) SendMail(addr string, a smtp.Auth, from string, to []string, msg []byte) error {
	m.mu.Lock()
	m.totalEnvios++
	tent := m.totalEnvios
	m.destinatarios = to
	m.ultimoCorpo = msg
	m.ultimoFrom = from
	delay := m.delayEnvio
	fn := m.erroDinamico
	m.mu.Unlock()

	if delay > 0 {
		time.Sleep(delay)
	}

	if fn != nil {
		return fn(tent)
	}
	return nil
}

func (m *mockSMTPSenderAvancado) Chamadas() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.totalEnvios
}

func TestEmailDemo_SucessoEPersistenciaSQLite(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "email_sucesso.db")
	repo, err := armazenamento.AbrirSQLite(context.Background(), "file:"+dbPath)
	require.NoError(t, err)
	defer repo.Fechar()

	// Cria o alerta no banco primeiro (devido à chave estrangeira)
	alertaID := "alt-sucesso-1"
	err = repo.SalvarAlerta(context.Background(), alerta.Alerta{
		ID:        alertaID,
		UsuariaID: "usuaria-teste",
		Status:    alerta.StatusProcessando,
		CriadoEm:  time.Now(),
	})
	require.NoError(t, err)

	metricas := observabilidade.NovaMetricas()
	mock := &mockSMTPSenderAvancado{}

	notif := notificacao.NovoEmailDemoNotificador(notificacao.ConfigEmailDemo{
		Ativo:                    true,
		Host:                     "smtp.teste.local",
		Porta:                    "587",
		Remetente:                "alerta@plataformadamulher.org",
		DestinatariosAutorizados: []string{"autorizado@teste.org"},
		MaxEnvios:                5,
		Timeout:                  2 * time.Second,
		ExigirTLS:                true,
		Sender:                   mock,
		Repo:                     repo,
		Metricas:                 metricas,
	})

	ctx := observabilidade.InjetarCorrelationID(context.Background(), "trace-email-sucesso-001")
	err = notif.EnviarAlertaDemonstrativo(ctx, alertaID, "autorizado@teste.org")
	require.NoError(t, err)

	assert.Equal(t, 1, mock.Chamadas())
	assert.Equal(t, []string{"autorizado@teste.org"}, mock.destinatarios)
	assert.Contains(t, string(mock.ultimoCorpo), alertaID)
	assert.Contains(t, string(mock.ultimoCorpo), "X-Correlation-ID: trace-email-sucesso-001")

	// Comprova persistência no SQLite
	jaEnviado, err := repo.EmailDemoJaEnviado(context.Background(), alertaID)
	require.NoError(t, err)
	assert.True(t, jaEnviado, "registro deve constar como enviado com sucesso no SQLite")

	// Comprova atualização de métricas do Prometheus
	assert.Equal(t, float64(1), testutil.ToFloat64(metricas.EmailDemoEnviosTotal.WithLabelValues("sucesso", "ok")))
}

func TestEmailDemo_FalhaIndisponibilidadeEAutenticacao(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "email_falha.db")
	repo, err := armazenamento.AbrirSQLite(context.Background(), "file:"+dbPath)
	require.NoError(t, err)
	defer repo.Fechar()

	alertaID := "alt-falha-auth-1"
	_ = repo.SalvarAlerta(context.Background(), alerta.Alerta{ID: alertaID, UsuariaID: "u1", CriadoEm: time.Now()})

	metricas := observabilidade.NovaMetricas()
	mock := &mockSMTPSenderAvancado{
		erroDinamico: func(tentativa int) error {
			return errors.New("535 5.7.8 Authentication credentials invalid")
		},
	}

	notif := notificacao.NovoEmailDemoNotificador(notificacao.ConfigEmailDemo{
		Ativo:                    true,
		Host:                     "smtp.teste.local",
		Remetente:                "remetente@teste.org",
		DestinatariosAutorizados: []string{"dest@teste.org"},
		Sender:                   mock,
		Repo:                     repo,
		Metricas:                 metricas,
	})

	err = notif.EnviarAlertaDemonstrativo(context.Background(), alertaID, "dest@teste.org")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Authentication credentials invalid")

	// Garante que o SQLite gravou a tentativa como falha
	jaEnviado, err := repo.EmailDemoJaEnviado(context.Background(), alertaID)
	require.NoError(t, err)
	assert.False(t, jaEnviado, "não pode constar como sucesso no SQLite")

	tentativas, err := repo.ContarTentativasEmailDemo(context.Background(), alertaID)
	require.NoError(t, err)
	assert.Equal(t, 1, tentativas)

	// Comprova métrica de falha por autenticação
	assert.Equal(t, float64(1), testutil.ToFloat64(metricas.EmailDemoEnviosTotal.WithLabelValues("falha", "auth")))
}

func TestEmailDemo_TimeoutECancelamentoContexto(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "email_timeout.db")
	repo, err := armazenamento.AbrirSQLite(context.Background(), "file:"+dbPath)
	require.NoError(t, err)
	defer repo.Fechar()

	alertaID := "alt-timeout-1"
	_ = repo.SalvarAlerta(context.Background(), alerta.Alerta{ID: alertaID, UsuariaID: "u1", CriadoEm: time.Now()})

	metricas := observabilidade.NovaMetricas()
	mock := &mockSMTPSenderAvancado{
		delayEnvio: 150 * time.Millisecond,
	}

	notif := notificacao.NovoEmailDemoNotificador(notificacao.ConfigEmailDemo{
		Ativo:                    true,
		Host:                     "smtp.teste.local",
		Remetente:                "remetente@teste.org",
		DestinatariosAutorizados: []string{"dest@teste.org"},
		Sender:                   mock,
		Repo:                     repo,
		Metricas:                 metricas,
	})

	ctxTimeout, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	err = notif.EnviarAlertaDemonstrativo(ctxTimeout, alertaID, "dest@teste.org")
	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)

	// Comprova métrica de falha por timeout
	assert.Equal(t, float64(1), testutil.ToFloat64(metricas.EmailDemoEnviosTotal.WithLabelValues("falha", "timeout")))
}

func TestEmailDemo_ResultadoIndeterminado(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "email_indeterminado.db")
	repo, err := armazenamento.AbrirSQLite(context.Background(), "file:"+dbPath)
	require.NoError(t, err)
	defer repo.Fechar()

	alertaID := "alt-indeterminado-1"
	_ = repo.SalvarAlerta(context.Background(), alerta.Alerta{ID: alertaID, UsuariaID: "u1", CriadoEm: time.Now()})

	metricas := observabilidade.NovaMetricas()
	mock := &mockSMTPSenderAvancado{
		erroDinamico: func(tentativa int) error {
			return fmt.Errorf("%w: timeout após envio do payload antes do ACK final", notificacao.ErrResultadoIndeterminado)
		},
	}

	notif := notificacao.NovoEmailDemoNotificador(notificacao.ConfigEmailDemo{
		Ativo:                    true,
		Host:                     "smtp.teste.local",
		Remetente:                "remetente@teste.org",
		DestinatariosAutorizados: []string{"dest@teste.org"},
		Sender:                   mock,
		Repo:                     repo,
		Metricas:                 metricas,
	})

	err = notif.EnviarAlertaDemonstrativo(context.Background(), alertaID, "dest@teste.org")
	require.Error(t, err)
	assert.ErrorIs(t, err, notificacao.ErrResultadoIndeterminado)

	// Comprova classificação como indeterminado na métrica
	assert.Equal(t, float64(1), testutil.ToFloat64(metricas.EmailDemoEnviosTotal.WithLabelValues("falha", "indeterminado")))
}

func TestEmailDemo_DestinatarioNaoAutorizado(t *testing.T) {
	metricas := observabilidade.NovaMetricas()
	mock := &mockSMTPSenderAvancado{}

	notif := notificacao.NovoEmailDemoNotificador(notificacao.ConfigEmailDemo{
		Ativo:                    true,
		Host:                     "smtp.teste.local",
		Remetente:                "remetente@teste.org",
		DestinatariosAutorizados: []string{"permitido@teste.org"},
		Sender:                   mock,
		Metricas:                 metricas,
	})

	// Tenta enviar para endereço fora da whitelist
	err := notif.EnviarAlertaDemonstrativo(context.Background(), "alt-01", "invasor@externo.com")
	require.Error(t, err)
	assert.ErrorIs(t, err, notificacao.ErrDestinatarioNaoPermitido)
	assert.Equal(t, 0, mock.Chamadas(), "jamais deve abrir conexão SMTP para destinatário não autorizado")

	// Comprova métrica de bloqueio
	assert.Equal(t, float64(1), testutil.ToFloat64(metricas.EmailDemoEnviosTotal.WithLabelValues("descartado", "destinatario_bloqueado")))
}

func TestEmailDemo_IdempotenciaPersistenteComReplay(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "email_idempotencia.db")
	repo, err := armazenamento.AbrirSQLite(context.Background(), "file:"+dbPath)
	require.NoError(t, err)
	defer repo.Fechar()

	alertaID := "alt-idempotencia-1"
	_ = repo.SalvarAlerta(context.Background(), alerta.Alerta{ID: alertaID, UsuariaID: "u1", CriadoEm: time.Now()})

	metricas := observabilidade.NovaMetricas()
	mock := &mockSMTPSenderAvancado{}

	cfg := notificacao.ConfigEmailDemo{
		Ativo:                    true,
		Host:                     "smtp.teste.local",
		Remetente:                "remetente@teste.org",
		DestinatariosAutorizados: []string{"dest@teste.org"},
		Sender:                   mock,
		Repo:                     repo,
		Metricas:                 metricas,
	}

	notifInstancia1 := notificacao.NovoEmailDemoNotificador(cfg)

	// 1. Primeiro envio tem sucesso
	err = notifInstancia1.EnviarAlertaDemonstrativo(context.Background(), alertaID, "dest@teste.org")
	require.NoError(t, err)
	assert.Equal(t, 1, mock.Chamadas())

	// 2. Simula restart da aplicação criando nova instância com cache em memória limpo
	notifInstancia2 := notificacao.NovoEmailDemoNotificador(cfg)

	// Segunda chamada para o mesmo alerta: o SQLite impede reenvio
	err = notifInstancia2.EnviarAlertaDemonstrativo(context.Background(), alertaID, "dest@teste.org")
	require.NoError(t, err)
	assert.Equal(t, 1, mock.Chamadas(), "não deve invocar SendMail novamente graças à persistência no SQLite")

	// Comprova métrica de replay ignorado
	assert.Equal(t, float64(1), testutil.ToFloat64(metricas.EmailDemoEnviosTotal.WithLabelValues("ignorado_idempotente", "ja_enviado")))
}

func TestEmailDemo_ConcorrenciaMultiplasGoroutines(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "email_concorrencia.db")
	repo, err := armazenamento.AbrirSQLite(context.Background(), "file:"+dbPath)
	require.NoError(t, err)
	defer repo.Fechar()

	alertaID := "alt-concorrente-1"
	_ = repo.SalvarAlerta(context.Background(), alerta.Alerta{ID: alertaID, UsuariaID: "u1", CriadoEm: time.Now()})

	mock := &mockSMTPSenderAvancado{
		delayEnvio: 10 * time.Millisecond,
	}

	notif := notificacao.NovoEmailDemoNotificador(notificacao.ConfigEmailDemo{
		Ativo:                    true,
		Host:                     "smtp.teste.local",
		Remetente:                "remetente@teste.org",
		DestinatariosAutorizados: []string{"dest@teste.org"},
		Sender:                   mock,
		Repo:                     repo,
	})

	const goroutines = 20
	var wg sync.WaitGroup
	var errosCount atomic.Int32

	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			if err := notif.EnviarAlertaDemonstrativo(context.Background(), alertaID, "dest@teste.org"); err != nil {
				errosCount.Add(1)
			}
		}()
	}

	wg.Wait()

	assert.Equal(t, int32(0), errosCount.Load())
	assert.Equal(t, 1, mock.Chamadas(), "exatamente 1 envio SMTP deve ocorrer mesmo com 20 goroutines concorrentes")
}

func TestEmailDemo_RecuperacaoERetentativasControladas(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "email_recuperacao.db")
	repo, err := armazenamento.AbrirSQLite(context.Background(), "file:"+dbPath)
	require.NoError(t, err)
	defer repo.Fechar()

	alertaID := "alt-recuperacao-1"
	_ = repo.SalvarAlerta(context.Background(), alerta.Alerta{ID: alertaID, UsuariaID: "u1", CriadoEm: time.Now()})

	metricas := observabilidade.NovaMetricas()
	var falhar atomic.Bool
	falhar.Store(true)

	mock := &mockSMTPSenderAvancado{
		erroDinamico: func(tentativa int) error {
			if falhar.Load() {
				return errors.New("421 Service not available, closing transmission channel")
			}
			return nil
		},
	}

	notif := notificacao.NovoEmailDemoNotificador(notificacao.ConfigEmailDemo{
		Ativo:                    true,
		Host:                     "smtp.teste.local",
		Remetente:                "remetente@teste.org",
		DestinatariosAutorizados: []string{"dest@teste.org"},
		Sender:                   mock,
		Repo:                     repo,
		Metricas:                 metricas,
	})

	// Tentativa 1: falha do servidor SMTP
	err = notif.EnviarAlertaDemonstrativo(context.Background(), alertaID, "dest@teste.org")
	require.Error(t, err)
	assert.Equal(t, 1, mock.Chamadas())

	// Recuperação do servidor SMTP
	falhar.Store(false)

	// Tentativa 2: servidor recuperado com sucesso
	err = notif.EnviarAlertaDemonstrativo(context.Background(), alertaID, "dest@teste.org")
	require.NoError(t, err)
	assert.Equal(t, 2, mock.Chamadas())

	// Verifica persistência de sucesso
	jaEnviado, err := repo.EmailDemoJaEnviado(context.Background(), alertaID)
	require.NoError(t, err)
	assert.True(t, jaEnviado)

	// Comprova métrica de retentativa agendada
	assert.Equal(t, float64(1), testutil.ToFloat64(metricas.EmailDemoRetentativasTotal.WithLabelValues("agendada")))
}

func TestEmailDemo_LimiteRetentativasExcedido(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "email_max_retries.db")
	repo, err := armazenamento.AbrirSQLite(context.Background(), "file:"+dbPath)
	require.NoError(t, err)
	defer repo.Fechar()

	alertaID := "alt-max-retries-1"
	_ = repo.SalvarAlerta(context.Background(), alerta.Alerta{ID: alertaID, UsuariaID: "u1", CriadoEm: time.Now()})

	metricas := observabilidade.NovaMetricas()
	mock := &mockSMTPSenderAvancado{
		erroDinamico: func(tentativa int) error {
			return errors.New("500 Permanent Failure")
		},
	}

	notif := notificacao.NovoEmailDemoNotificador(notificacao.ConfigEmailDemo{
		Ativo:                    true,
		Host:                     "smtp.teste.local",
		Remetente:                "remetente@teste.org",
		DestinatariosAutorizados: []string{"dest@teste.org"},
		Sender:                   mock,
		Repo:                     repo,
		Metricas:                 metricas,
	})

	// Executa 3 falhas seguidas
	for i := 1; i <= 3; i++ {
		_ = notif.EnviarAlertaDemonstrativo(context.Background(), alertaID, "dest@teste.org")
	}
	assert.Equal(t, 3, mock.Chamadas())

	// 4ª chamada: limite de 3 tentativas atingido, conclui sem reenviar
	err = notif.EnviarAlertaDemonstrativo(context.Background(), alertaID, "dest@teste.org")
	assert.NoError(t, err, "ao atingir o limite de retentativas, não deve falhar nem travar o consumidor")
	assert.Equal(t, 3, mock.Chamadas(), "não deve invocar SendMail pela 4ª vez")

	// Comprova métrica de limite excedido
	assert.Equal(t, float64(1), testutil.ToFloat64(metricas.EmailDemoRetentativasTotal.WithLabelValues("limite_excedido")))
}
