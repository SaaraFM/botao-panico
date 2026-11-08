package fila_test

import (
	"context"
	"errors"
	"net/smtp"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/plataforma-mulher/botao-panico/internal/alerta"
	"github.com/plataforma-mulher/botao-panico/internal/armazenamento"
	"github.com/plataforma-mulher/botao-panico/internal/notificacao"
	"github.com/plataforma-mulher/botao-panico/internal/observabilidade"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockSMTPSenderFila struct {
	mu          sync.Mutex
	totalEnvios int
	erroRetorno error
}

func (m *mockSMTPSenderFila) SendMail(addr string, a smtp.Auth, from string, to []string, msg []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.totalEnvios++
	return m.erroRetorno
}

func TestConsumidorNotificacoes_EmailDemoFalhaNaoBloqueiaAlerta(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "consumidor_email_falha.db")
	repo, err := armazenamento.AbrirSQLite(context.Background(), "file:"+dbPath)
	require.NoError(t, err)
	defer repo.Fechar()

	alertaID := "alt-consumidor-email-1"
	err = repo.SalvarAlerta(context.Background(), alerta.Alerta{
		ID:        alertaID,
		UsuariaID: "usuaria-teste",
		Nivel:     alerta.RiscoAlto,
		Status:    alerta.StatusProcessando,
		CriadoEm:  time.Now(),
	})
	require.NoError(t, err)

	metricas := observabilidade.NovaMetricas()
	mockSMTP := &mockSMTPSenderFila{
		erroRetorno: errors.New("smtp: dial tcp connection refused"),
	}

	emailDemo := notificacao.NovoEmailDemoNotificador(notificacao.ConfigEmailDemo{
		Ativo:                    true,
		Host:                     "smtp.invalido.local",
		Remetente:                "alerta@demo.org",
		DestinatariosAutorizados: []string{"autorizado@demo.org"},
		Sender:                   mockSMTP,
		Repo:                     repo,
		Metricas:                 metricas,
	})

	despachante := &despachanteMock{}
	servico := alerta.NovoServico(repo, despachante, nil, time.Second)

	// Simula a execução do consumidor para esta tarefa de notificação
	ctxProc := observabilidade.InjetarCorrelationID(context.Background(), "trace-teste-consumidor-001")

	// 1. Processa tarefa principal de notificação do alerta
	err = servico.ProcessarTarefaNotificacao(ctxProc, alertaID)
	require.NoError(t, err)

	// 2. Dispara e-mail demonstrativo (com falha simulada do provedor)
	errEmail := emailDemo.EnviarAlertaDemonstrativo(ctxProc, alertaID, "autorizado@demo.org")
	require.Error(t, errEmail, "o provedor simulado falhou")

	// 3. Comprova que o alerta PERMANECE SALVO e com status CONCLUÍDO no SQLite
	alertaRecuperado, err := repo.BuscarAlerta(context.Background(), alertaID)
	require.NoError(t, err)
	assert.Equal(t, alerta.StatusConcluido, alertaRecuperado.Status, "o alerta deve ser preservado como concluído apesar da falha externa")

	// 4. Comprova registro da falha do e-mail no SQLite
	tentativas, err := repo.ContarTentativasEmailDemo(context.Background(), alertaID)
	require.NoError(t, err)
	assert.Equal(t, 1, tentativas)

	jaEnviado, err := repo.EmailDemoJaEnviado(context.Background(), alertaID)
	require.NoError(t, err)
	assert.False(t, jaEnviado, "não pode constar como sucesso")
}

func TestConsumidorNotificacoes_EmailDemoSucessoRegistrado(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "consumidor_email_sucesso.db")
	repo, err := armazenamento.AbrirSQLite(context.Background(), "file:"+dbPath)
	require.NoError(t, err)
	defer repo.Fechar()

	alertaID := "alt-consumidor-email-2"
	err = repo.SalvarAlerta(context.Background(), alerta.Alerta{
		ID:        alertaID,
		UsuariaID: "usuaria-teste",
		Nivel:     alerta.RiscoAlto,
		Status:    alerta.StatusProcessando,
		CriadoEm:  time.Now(),
	})
	require.NoError(t, err)

	mockSMTP := &mockSMTPSenderFila{}
	metricas := observabilidade.NovaMetricas()

	emailDemo := notificacao.NovoEmailDemoNotificador(notificacao.ConfigEmailDemo{
		Ativo:                    true,
		Host:                     "smtp.valido.local",
		Remetente:                "alerta@demo.org",
		DestinatariosAutorizados: []string{"autorizado@demo.org"},
		Sender:                   mockSMTP,
		Repo:                     repo,
		Metricas:                 metricas,
	})

	despachante := &despachanteMock{}
	servico := alerta.NovoServico(repo, despachante, nil, time.Second)

	ctxProc := observabilidade.InjetarCorrelationID(context.Background(), "trace-teste-consumidor-002")

	// Processa notificação e envia e-mail demonstrativo
	require.NoError(t, servico.ProcessarTarefaNotificacao(ctxProc, alertaID))
	require.NoError(t, emailDemo.EnviarAlertaDemonstrativo(ctxProc, alertaID, "autorizado@demo.org"))

	// Comprova sucesso registrado no SQLite
	jaEnviado, err := repo.EmailDemoJaEnviado(context.Background(), alertaID)
	require.NoError(t, err)
	assert.True(t, jaEnviado)

	// Reenvio não deve invocar SendMail novamente
	require.NoError(t, emailDemo.EnviarAlertaDemonstrativo(ctxProc, alertaID, "autorizado@demo.org"))
	assert.Equal(t, 1, mockSMTP.totalEnvios)
}
