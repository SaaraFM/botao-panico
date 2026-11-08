package notificacao

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/smtp"
	"strings"
	"sync"
	"time"

	"github.com/plataforma-mulher/botao-panico/internal/alerta"
	"github.com/plataforma-mulher/botao-panico/internal/config"
	"github.com/plataforma-mulher/botao-panico/internal/observabilidade"
)

// Erros específicos do modo de demonstração.
var (
	ErrEmailDemoDesativado     = errors.New("modo de e-mail demonstrativo real está desativado por padrão")
	ErrDestinatarioNaoPermitido = errors.New("envio bloqueado: destinatário não autorizado na lista de testes")
	ErrLimiteEnvioAtingido     = errors.New("limite de segurança de envios demonstrativos por hora atingido")
	ErrResultadoIndeterminado  = errors.New("resultado de entrega SMTP indeterminado: timeout ou desconexão durante confirmação")
)

// SMTPSender define a interface para envio via protocolo SMTP (permite injeção de mocks nos testes).
type SMTPSender interface {
	SendMail(addr string, a smtp.Auth, from string, to []string, msg []byte) error
}

// RepositorioNotificacoes define as operações necessárias no banco de dados para controle de idempotência e histórico.
type RepositorioNotificacoes interface {
	SalvarNotificacao(ctx context.Context, alertaID string, n alerta.ResultadoEnvio) error
	EmailDemoJaEnviado(ctx context.Context, alertaID string) (bool, error)
	ContarTentativasEmailDemo(ctx context.Context, alertaID string) (int, error)
}

// senderPadrao implementa o envio SMTP com suporte estrito a criptografia TLS/STARTTLS,
// validação de certificado do servidor e timeouts configuráveis.
type senderPadrao struct {
	timeout   time.Duration
	exigirTLS bool
}

func (s senderPadrao) SendMail(addr string, a smtp.Auth, from string, to []string, msg []byte) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
		port = "587"
	}

	timeout := s.timeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}

	dialer := &net.Dialer{Timeout: timeout}
	conn, err := dialer.Dial("tcp", net.JoinHostPort(host, port))
	if err != nil {
		return fmt.Errorf("conexão TCP com %s falhou: %w", addr, err)
	}
	defer conn.Close()

	// Se porta 465 (SMTPS direto)
	if port == "465" {
		tlsConn := tls.Client(conn, &tls.Config{
			ServerName: host,
			MinVersion: tls.VersionTLS12,
		})
		if err := tlsConn.Handshake(); err != nil {
			return fmt.Errorf("handshake TLS direto falhou: %w", err)
		}
		conn = tlsConn
	}

	client, err := smtp.NewClient(conn, host)
	if err != nil {
		return fmt.Errorf("inicialização do cliente SMTP falhou: %w", err)
	}
	defer client.Close()

	// STARTTLS se não for porta 465
	if port != "465" {
		if ok, _ := client.Extension("STARTTLS"); ok {
			tlsConfig := &tls.Config{
				ServerName: host,
				MinVersion: tls.VersionTLS12,
			}
			if err := client.StartTLS(tlsConfig); err != nil {
				return fmt.Errorf("STARTTLS falhou (certificado inválido ou recusa): %w", err)
			}
		} else if s.exigirTLS {
			return errors.New("servidor SMTP não suporta STARTTLS obrigatório")
		}
	}

	if a != nil {
		if ok, _ := client.Extension("AUTH"); ok {
			if err := client.Auth(a); err != nil {
				return fmt.Errorf("autenticação SMTP falhou: %w", err)
			}
		}
	}

	if err := client.Mail(from); err != nil {
		return fmt.Errorf("comando MAIL FROM falhou: %w", err)
	}

	for _, rec := range to {
		if err := client.Rcpt(rec); err != nil {
			return fmt.Errorf("comando RCPT TO falhou para %s: %w", rec, err)
		}
	}

	w, err := client.Data()
	if err != nil {
		return fmt.Errorf("comando DATA falhou: %w", err)
	}

	if _, err = w.Write(msg); err != nil {
		_ = w.Close()
		return fmt.Errorf("%w: falha ao escrever corpo: %v", ErrResultadoIndeterminado, err)
	}

	if err := w.Close(); err != nil {
		return fmt.Errorf("%w: falha ao fechar DATA: %v", ErrResultadoIndeterminado, err)
	}

	return client.Quit()
}

// ConfigEmailDemo contém os parâmetros necessários para o envio acadêmico controlado.
type ConfigEmailDemo struct {
	Ativo                    bool
	Host                     string
	Porta                    string
	Usuario                  string
	Senha                    string
	Remetente                string
	DestinatariosAutorizados []string
	MaxEnvios                int           // limite máximo de segurança por hora (padrão: 5)
	Timeout                  time.Duration // timeout de socket SMTP
	ExigirTLS                bool          // obrigatoriedade de TLS
	Sender                   SMTPSender    // opcional: se nil usa senderPadrao
	Repo                     RepositorioNotificacoes
	Metricas                 *observabilidade.Metricas
}

// EmailDemoNotificador implementa envio restrito e seguro para demonstrações acadêmicas.
type EmailDemoNotificador struct {
	cfg      ConfigEmailDemo
	mu       sync.Mutex
	envios   []time.Time
	enviados map[string]bool // rastreia alertas já enviados nesta execução
}

// NovoEmailDemoNotificador instancia o notificador de demonstração.
func NovoEmailDemoNotificador(cfg ConfigEmailDemo) *EmailDemoNotificador {
	if cfg.Porta == "" {
		cfg.Porta = "587"
	}
	if cfg.MaxEnvios <= 0 {
		cfg.MaxEnvios = 5
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 10 * time.Second
	}
	if cfg.Sender == nil {
		cfg.Sender = senderPadrao{
			timeout:   cfg.Timeout,
			exigirTLS: cfg.ExigirTLS,
		}
	}
	if len(cfg.DestinatariosAutorizados) == 0 {
		cfg.DestinatariosAutorizados = []string{strings.ToLower(config.EmailAutorizadoDemo)}
	}

	normDests := make([]string, 0, len(cfg.DestinatariosAutorizados))
	for _, d := range cfg.DestinatariosAutorizados {
		dLimpo := strings.ToLower(strings.TrimSpace(d))
		if dLimpo != "" {
			normDests = append(normDests, dLimpo)
		}
	}
	cfg.DestinatariosAutorizados = normDests

	return &EmailDemoNotificador{
		cfg:      cfg,
		envios:   make([]time.Time, 0),
		enviados: make(map[string]bool),
	}
}

func (e *EmailDemoNotificador) Canal() string {
	return "email_demo_real"
}

// DestinatarioAutorizado verifica se o e-mail informado consta na whitelist configurada.
func (e *EmailDemoNotificador) DestinatarioAutorizado(email string) bool {
	emailNorm := strings.ToLower(strings.TrimSpace(email))
	for _, a := range e.cfg.DestinatariosAutorizados {
		if emailNorm == a {
			return true
		}
	}
	return false
}

// EnviarAlertaDemonstrativo despacha um e-mail com texto estritamente demonstrativo
// para a lista autorizada de destinatários de teste.
//
// Garante idempotência em duas camadas (memória e SQLite), preservação do alerta em caso de falha,
// rastreamento de Correlation ID e registro de métricas sem exposição de dados sensíveis.
func (e *EmailDemoNotificador) EnviarAlertaDemonstrativo(ctx context.Context, alertaID string, destinatarios ...string) error {
	if !e.cfg.Ativo {
		return ErrEmailDemoDesativado
	}

	// 1. Definição e validação estrita dos destinatários
	destsParaEnvio := destinatarios
	if len(destsParaEnvio) == 0 {
		destsParaEnvio = e.cfg.DestinatariosAutorizados
	}

	for _, d := range destsParaEnvio {
		if !e.DestinatarioAutorizado(d) {
			slog.Warn("tentativa de envio para destinatário não autorizado bloqueada",
				"alerta_id", alertaID,
				"destinatario_bloqueado", d,
			)
			if e.cfg.Metricas != nil {
				e.cfg.Metricas.RegistrarEnvioEmailDemo("descartado", "destinatario_bloqueado", 0)
			}
			return fmt.Errorf("%w: %s", ErrDestinatarioNaoPermitido, d)
		}
	}

	corrID := observabilidade.ObterCorrelationID(ctx)

	e.mu.Lock()
	defer e.mu.Unlock()

	// 2. Idempotência em memória (Camada 1)
	if e.enviados[alertaID] {
		slog.Info("e-mail demonstrativo já enviado anteriormente nesta execução; idempotência respeitada",
			"alerta_id", alertaID,
			"correlation_id", corrID,
		)
		if e.cfg.Metricas != nil {
			e.cfg.Metricas.RegistrarEnvioEmailDemo("ignorado_idempotente", "ja_enviado", 0)
		}
		return nil
	}

	// 3. Idempotência persistente no SQLite (Camada 2)
	if e.cfg.Repo != nil {
		jaEnviado, err := e.cfg.Repo.EmailDemoJaEnviado(ctx, alertaID)
		if err == nil && jaEnviado {
			e.enviados[alertaID] = true
			slog.Info("e-mail demonstrativo já registrado com sucesso no banco de dados; ignorando redespacho",
				"alerta_id", alertaID,
				"correlation_id", corrID,
			)
			if e.cfg.Metricas != nil {
				e.cfg.Metricas.RegistrarEnvioEmailDemo("ignorado_idempotente", "ja_enviado", 0)
			}
			return nil
		}

		// Controle de retentativas máximas (máximo 3 tentativas para evitar loops)
		tentativas, errCont := e.cfg.Repo.ContarTentativasEmailDemo(ctx, alertaID)
		if errCont == nil && tentativas >= 3 {
			slog.Warn("limite de retentativas para e-mail demonstrativo atingido; envio descartado sem bloquear alerta",
				"alerta_id", alertaID,
				"correlation_id", corrID,
				"tentativas", tentativas,
			)
			if e.cfg.Metricas != nil {
				e.cfg.Metricas.RegistrarRetentativaEmailDemo("limite_excedido")
			}
			return nil
		}
		if errCont == nil && tentativas > 0 && e.cfg.Metricas != nil {
			e.cfg.Metricas.RegistrarRetentativaEmailDemo("agendada")
		}
	}

	// 4. Rate Limiting por hora (janela deslizante)
	agora := time.Now()
	var janela []time.Time
	for _, t := range e.envios {
		if agora.Sub(t) < time.Hour {
			janela = append(janela, t)
		}
	}
	e.envios = janela
	if len(e.envios) >= e.cfg.MaxEnvios {
		slog.Warn("limite de segurança de e-mails demonstrativos por hora atingido",
			"alerta_id", alertaID,
			"correlation_id", corrID,
			"limite_hora", e.cfg.MaxEnvios,
		)
		if e.cfg.Metricas != nil {
			e.cfg.Metricas.RegistrarEnvioEmailDemo("descartado", "limite_taxa", 0)
		}
		return fmt.Errorf("%w: máximo de %d e-mails por hora", ErrLimiteEnvioAtingido, e.cfg.MaxEnvios)
	}

	// 5. Montagem do payload puramente demonstrativo (SEM PII, coordenadas, telefones ou nomes reais)
	assunto := "[TESTE ACADÊMICO] Alerta de Pânico — Plataforma da Mulher (Demonstração)"
	corpo := fmt.Sprintf(
		"Este é um e-mail de demonstração técnica acadêmica do sistema de Botão de Pânico (Plataforma da Mulher).\r\n\r\n"+
			"Identificador do Alerta: %s\r\n"+
			"Data e Hora do Evento: %s UTC\r\n"+
			"Status no Sistema: Processado e Auditado\r\n\r\n"+
			"AVISO DE SEGURANÇA:\r\n"+
			"Esta mensagem foi gerada automaticamente em ambiente controlado de testes acadêmicos (CIT7219).\r\n"+
			"Não representa uma situação real de emergência e não requer providências operacionais de socorro.",
		alertaID,
		agora.UTC().Format(time.RFC3339),
	)

	destsString := strings.Join(destsParaEnvio, ", ")
	msg := []byte(fmt.Sprintf(
		"From: %s\r\n"+
			"To: %s\r\n"+
			"Subject: %s\r\n"+
			"X-Correlation-ID: %s\r\n"+
			"MIME-Version: 1.0\r\n"+
			"Content-Type: text/plain; charset=UTF-8\r\n"+
			"\r\n%s",
		e.cfg.Remetente,
		destsString,
		assunto,
		corrID,
		corpo,
	))

	var auth smtp.Auth
	if e.cfg.Usuario != "" && e.cfg.Senha != "" {
		senhaLimpa := strings.ReplaceAll(e.cfg.Senha, " ", "")
		auth = smtp.PlainAuth("", e.cfg.Usuario, senhaLimpa, e.cfg.Host)
	}

	enderecoServidor := e.cfg.Host + ":" + e.cfg.Porta

	// 6. Envio SMTP com controle de timeout e cancelamento
	inicio := time.Now()
	done := make(chan error, 1)

	go func() {
		done <- e.cfg.Sender.SendMail(enderecoServidor, auth, e.cfg.Remetente, destsParaEnvio, msg)
	}()

	var erroEnvio error
	select {
	case <-ctx.Done():
		erroEnvio = ctx.Err()
	case err := <-done:
		erroEnvio = err
	}

	duracao := time.Since(inicio)

	// 7. Tratamento de Erro e Persistência no SQLite
	if erroEnvio != nil {
		motivo := "conexao"
		if errors.Is(erroEnvio, context.Canceled) || errors.Is(erroEnvio, context.DeadlineExceeded) {
			motivo = "timeout"
		} else if strings.Contains(strings.ToLower(erroEnvio.Error()), "auth") {
			motivo = "auth"
		} else if errors.Is(erroEnvio, ErrResultadoIndeterminado) {
			motivo = "indeterminado"
		}

		if e.cfg.Metricas != nil {
			e.cfg.Metricas.RegistrarEnvioEmailDemo("falha", motivo, duracao.Seconds())
		}

		slog.Error("falha no envio de e-mail demonstrativo SMTP",
			"alerta_id", alertaID,
			"correlation_id", corrID,
			"motivo", motivo,
			"duracao_ms", duracao.Milliseconds(),
			"erro", erroEnvio.Error(),
		)

		if e.cfg.Repo != nil {
			_ = e.cfg.Repo.SalvarNotificacao(ctx, alertaID, alerta.ResultadoEnvio{
				Destino:   destsString,
				Canal:     "email_demo_real",
				Sucesso:   false,
				Erro:      erroEnvio.Error(),
				DuracaoMs: duracao.Milliseconds(),
				EnviadoEm: time.Now().UTC().Format(time.RFC3339),
			})
		}

		return fmt.Errorf("envio smtp demonstrativo falhou: %w", erroEnvio)
	}

	// 8. Sucesso: atualiza janelas, deduplicação e registra no SQLite
	e.envios = append(e.envios, agora)
	e.enviados[alertaID] = true

	if e.cfg.Metricas != nil {
		e.cfg.Metricas.RegistrarEnvioEmailDemo("sucesso", "ok", duracao.Seconds())
	}

	if e.cfg.Repo != nil {
		_ = e.cfg.Repo.SalvarNotificacao(ctx, alertaID, alerta.ResultadoEnvio{
			Destino:   destsString,
			Canal:     "email_demo_real",
			Sucesso:   true,
			DuracaoMs: duracao.Milliseconds(),
			EnviadoEm: time.Now().UTC().Format(time.RFC3339),
		})
	}

	slog.Info("e-mail demonstrativo acadêmico aceito pelo servidor SMTP",
		"alerta_id", alertaID,
		"correlation_id", corrID,
		"destinatarios", destsString,
		"duracao_ms", duracao.Milliseconds(),
	)

	return nil
}
