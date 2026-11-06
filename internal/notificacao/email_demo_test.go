package notificacao_test

import (
	"context"
	"net/smtp"
	"sync"
	"testing"

	"github.com/plataforma-mulher/botao-panico/internal/config"
	"github.com/plataforma-mulher/botao-panico/internal/notificacao"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockSMTPSender struct {
	mu            sync.Mutex
	chamadas      int
	destinatarios []string
	ultimoCorpo   []byte
	erro          error
}

func (m *mockSMTPSender) SendMail(addr string, a smtp.Auth, from string, to []string, msg []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.chamadas++
	m.destinatarios = to
	m.ultimoCorpo = msg
	return m.erro
}

func TestEmailDemo_DesativadoPorPadrao(t *testing.T) {
	mock := &mockSMTPSender{}
	notif := notificacao.NovoEmailDemoNotificador(notificacao.ConfigEmailDemo{
		Ativo:  false,
		Sender: mock,
	})

	err := notif.EnviarAlertaDemonstrativo(context.Background(), "alerta-teste-123")
	require.Error(t, err)
	assert.ErrorIs(t, err, notificacao.ErrEmailDemoDesativado)
	assert.Equal(t, 0, mock.chamadas, "nenhum envio SMTP deve ocorrer quando desativado")
}

func TestEmailDemo_DestinatarioEstritamenteAutorizado(t *testing.T) {
	mock := &mockSMTPSender{}
	notif := notificacao.NovoEmailDemoNotificador(notificacao.ConfigEmailDemo{
		Ativo:     true,
		Host:      "smtp.demo.local",
		Porta:     "587",
		Remetente: "noreply@demo.local",
		Sender:    mock,
		MaxEnvios: 5,
	})

	err := notif.EnviarAlertaDemonstrativo(context.Background(), "alerta-demo-999")
	require.NoError(t, err)
	assert.Equal(t, 1, mock.chamadas)
	require.Len(t, mock.destinatarios, 1)
	assert.Equal(t, config.EmailAutorizadoDemo, mock.destinatarios[0], "destinatário deve ser exclusivamente o autorizado")
	assert.Contains(t, string(mock.ultimoCorpo), "alerta-demo-999")
	assert.Contains(t, string(mock.ultimoCorpo), "[TESTE ACADÊMICO]")
	assert.Contains(t, string(mock.ultimoCorpo), "Não representa uma situação real de emergência")
}

func TestEmailDemo_ProtecaoContraDuplicatasELimite(t *testing.T) {
	mock := &mockSMTPSender{}
	notif := notificacao.NovoEmailDemoNotificador(notificacao.ConfigEmailDemo{
		Ativo:     true,
		Host:      "smtp.demo.local",
		Sender:    mock,
		MaxEnvios: 2,
	})

	// Primeiro envio do alerta 1
	err := notif.EnviarAlertaDemonstrativo(context.Background(), "alerta-001")
	require.NoError(t, err)
	assert.Equal(t, 1, mock.chamadas)

	// Reenvio do mesmo alerta 1 não deve disparar novo SMTP
	err = notif.EnviarAlertaDemonstrativo(context.Background(), "alerta-001")
	require.NoError(t, err)
	assert.Equal(t, 1, mock.chamadas, "alerta duplicado não deve gerar novo envio")

	// Envio de alerta 2
	err = notif.EnviarAlertaDemonstrativo(context.Background(), "alerta-002")
	require.NoError(t, err)
	assert.Equal(t, 2, mock.chamadas)

	// Envio de alerta 3 atinge o limite de 2 por hora
	err = notif.EnviarAlertaDemonstrativo(context.Background(), "alerta-003")
	require.Error(t, err)
	assert.ErrorIs(t, err, notificacao.ErrLimiteEnvioAtingido)
	assert.Equal(t, 2, mock.chamadas)
}

func TestEmailDemo_CancelamentoContexto(t *testing.T) {
	mock := &mockSMTPSender{}
	notif := notificacao.NovoEmailDemoNotificador(notificacao.ConfigEmailDemo{
		Ativo:  true,
		Host:   "smtp.demo.local",
		Sender: mock,
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancelado imediatamente

	err := notif.EnviarAlertaDemonstrativo(ctx, "alerta-cancelado")
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}
