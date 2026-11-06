package notificacao

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/smtp"
	"sync"
	"time"

	"github.com/plataforma-mulher/botao-panico/internal/config"
)

// Erros específicos do modo de demonstração.
var (
	ErrEmailDemoDesativado      = errors.New("modo de e-mail demonstrativo real está desativado por padrão")
	ErrDestinatarioNaoPermitido = errors.New("envio bloqueado: destinatário não autorizado para testes acadêmicos")
	ErrLimiteEnvioAtingido      = errors.New("limite de segurança de envios demonstrativos atingido")
)

// SMTPSender define a interface para envio via protocolo SMTP (permite injeção de mocks nos testes).
type SMTPSender interface {
	SendMail(addr string, a smtp.Auth, from string, to []string, msg []byte) error
}

type senderPadrao struct{}

func (senderPadrao) SendMail(addr string, a smtp.Auth, from string, to []string, msg []byte) error {
	return smtp.SendMail(addr, a, from, to, msg)
}

// ConfigEmailDemo contém os parâmetros necessários para o envio acadêmico controlado.
type ConfigEmailDemo struct {
	Ativo     bool
	Host      string
	Porta     string
	Usuario   string
	Senha     string
	Remetente string
	Sender    SMTPSender // opcional: se nil usa senderPadrao
	MaxEnvios int        // limite máximo de segurança por hora (padrão: 5)
}

// EmailDemoNotificador implementa envio restrito e seguro para demonstrações acadêmicas.
type EmailDemoNotificador struct {
	cfg      ConfigEmailDemo
	mu       sync.Mutex
	envios   []time.Time
	enviados map[string]bool // rastreia protocolos para evitar reenvio do mesmo alerta
}

// NovoEmailDemoNotificador instancia o notificador de demonstração.
func NovoEmailDemoNotificador(cfg ConfigEmailDemo) *EmailDemoNotificador {
	if cfg.Porta == "" {
		cfg.Porta = "587"
	}
	if cfg.Sender == nil {
		cfg.Sender = senderPadrao{}
	}
	if cfg.MaxEnvios <= 0 {
		cfg.MaxEnvios = 5
	}
	return &EmailDemoNotificador{
		cfg:      cfg,
		envios:   make([]time.Time, 0),
		enviados: make(map[string]bool),
	}
}

func (e *EmailDemoNotificador) Canal() string {
	return "email_demo_real"
}

// EnviarAlertaDemonstrativo despacha um e-mail com texto estritamente fictício para a caixa oficial de teste.
func (e *EmailDemoNotificador) EnviarAlertaDemonstrativo(ctx context.Context, protocoloFicticio string) error {
	if !e.cfg.Ativo {
		return ErrEmailDemoDesativado
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	// 1. Verificação de reenvio duplicado do mesmo alerta
	if e.enviados[protocoloFicticio] {
		slog.Warn("envio de e-mail demonstrativo duplicado ignorado", "protocolo", protocoloFicticio)
		return nil
	}

	// 2. Verificação de limite de segurança por hora
	agora := time.Now()
	var janela []time.Time
	for _, t := range e.envios {
		if agora.Sub(t) < time.Hour {
			janela = append(janela, t)
		}
	}
	e.envios = janela
	if len(e.envios) >= e.cfg.MaxEnvios {
		return fmt.Errorf("%w: máximo de %d e-mails por hora", ErrLimiteEnvioAtingido, e.cfg.MaxEnvios)
	}

	// 3. Montagem da mensagem segura com dados estritamente fictícios
	assunto := "[TESTE ACADÊMICO] Alerta de Pânico — Plataforma da Mulher"
	corpo := fmt.Sprintf(
		"Um alerta fictício foi registrado durante uma demonstração acadêmica da Plataforma da Mulher.\r\n\r\n"+
			"Protocolo: %s\r\n"+
			"Status: Registro efetuado para demonstração.\r\n"+
			"Data/Hora: %s UTC\r\n\r\n"+
			"Esta mensagem é gerada em ambiente controlado de teste. Não representa uma situação real de emergência e não exige atendimento.",
		protocoloFicticio,
		agora.UTC().Format(time.RFC3339),
	)

	msg := []byte(fmt.Sprintf(
		"From: %s\r\n"+
			"To: %s\r\n"+
			"Subject: %s\r\n"+
			"MIME-Version: 1.0\r\n"+
			"Content-Type: text/plain; charset=UTF-8\r\n"+
			"\r\n%s",
		e.cfg.Remetente,
		config.EmailAutorizadoDemo,
		assunto,
		corpo,
	))

	var auth smtp.Auth
	if e.cfg.Usuario != "" && e.cfg.Senha != "" {
		auth = smtp.PlainAuth("", e.cfg.Usuario, e.cfg.Senha, e.cfg.Host)
	}

	enderecoServidor := e.cfg.Host + ":" + e.cfg.Porta

	// Envio com suporte a timeout/cancelamento via goroutine
	done := make(chan error, 1)
	go func() {
		done <- e.cfg.Sender.SendMail(enderecoServidor, auth, e.cfg.Remetente, []string{config.EmailAutorizadoDemo}, msg)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-done:
		if err != nil {
			slog.Error("falha no envio de e-mail demonstrativo SMTP", "erro", err)
			return fmt.Errorf("envio smtp demonstrativo falhou: %w", err)
		}
		e.envios = append(e.envios, agora)
		e.enviados[protocoloFicticio] = true
		slog.Info("e-mail demonstrativo acadêmico aceito pelo servidor SMTP",
			"destinatario_autorizado", config.EmailAutorizadoDemo,
			"protocolo", protocoloFicticio,
		)
		return nil
	}
}
