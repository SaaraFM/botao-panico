// Package config carrega e valida a configuração a partir de variáveis de ambiente.
package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// EmailAutorizadoDemo é o único endereço permitido para envio demonstrativo em ambiente acadêmico.
// Nenhum outro endereço será aceito, garantindo segurança estrita.
const EmailAutorizadoDemo = "plataformadamulheroficial@gmail.com"

// Config reúne todos os parâmetros do microserviço.
type Config struct {
	Porta               string        // PORT
	PortaGRPC           string        // GRPC_PORT (porta do servidor gRPC)
	HostGRPC            string        // GRPC_HOST (padrão seguro localhost: 127.0.0.1)
	URLBanco            string        // DATABASE_URL (DSN do SQLite)
	URLAPIExterna       string        // EXTERNAL_API_URL (vazio desativa a geocodificação)
	TimeoutAPIExterna   time.Duration // EXTERNAL_API_TIMEOUT
	AgenteAPIExterna    string        // EXTERNAL_API_USER_AGENT
	TimeoutNotificacoes time.Duration // NOTIFICATION_TIMEOUT
	ChaveServico        string        // SERVICE_KEY (chave serviço-a-serviço)
	ExigirAutenticacao  bool          // EXIGIR_AUTENTICACAO (falha se não houver SERVICE_KEY válida)

	// Configurações de Mensageria RabbitMQ AMQP 0-9-1 (Semana 13 CIT7219)
	FilasAtivas            bool          // FILAS_ATIVAS (habilita RabbitMQ e Outbox assíncrono)
	AMQPURL                string        // AMQP_URL (ex.: amqp://guest:guest@127.0.0.1:5672/)
	ExchangeEventos        string        // RABBITMQ_EXCHANGE (padrão: panico.eventos)
	ExchangeDLX            string        // RABBITMQ_DLX (padrão: panico.eventos.dlx)
	FilaNotificacoes       string        // RABBITMQ_QUEUE_NOTIFICACOES (padrão: panico.alertas.notificacoes)
	RoutingKeyNotificacoes string        // RABBITMQ_ROUTING_KEY_NOTIFICACOES (padrão: alerta.criado)
	FilaAuditoria          string        // RABBITMQ_QUEUE_AUDITORIA (padrão: panico.alertas.auditoria)
	RoutingKeyAuditoria    string        // RABBITMQ_ROUTING_KEY_AUDITORIA (padrão: #)
	FilaDLQ                string        // RABBITMQ_QUEUE_DLQ (padrão: panico.alertas.dlq)
	TimeoutWorker          time.Duration // WORKER_SHUTDOWN_TIMEOUT

	// Configurações de E-mail Demonstrativo Acadêmico (FASE 6 - Desativado por padrão)
	EmailDemoAtivo bool   // EMAIL_DEMO_ATIVO (padrão: false)
	SMTPHost       string // SMTP_HOST
	SMTPPorta      string // SMTP_PORT (padrão: 587)
	SMTPUsuario    string // SMTP_USER
	SMTPSenha      string // SMTP_PASS
	SMTPRemetente  string // SMTP_FROM

	// Configurações de Resiliência e Circuit Breaker para APIs externas (Nominatim)
	CircuitBreakerMaxFalhas          int           // CIRCUIT_BREAKER_MAX_FALHAS (padrão: 3)
	CircuitBreakerTimeoutRecuperacao time.Duration // CIRCUIT_BREAKER_TIMEOUT_RECUPERACAO (padrão: 10s)
	NominatimIntervaloMinimo         time.Duration // NOMINATIM_INTERVALO_MINIMO (padrão: 1s, política OSM)
}

// Valores padrão para desenvolvimento local.
const (
	PortaPadrao                            = "8080"
	PortaIntegracao                        = "8081" // Porta recomendada para integração com Plataforma da Mulher
	PortaGRPCPadrao                        = "50051"
	HostGRPCPadrao                         = "127.0.0.1" // Padrão seguro restrito a localhost
	AMQPURLPadrao                          = "amqp://guest:guest@127.0.0.1:5672/"
	ExchangeEventosPadrao                  = "panico.eventos"
	ExchangeDLXPadrao                      = "panico.eventos.dlx"
	FilaNotificacoesPadrao                 = "panico.alertas.notificacoes"
	RoutingKeyNotificacoesPadrao           = "alerta.criado"
	FilaAuditoriaPadrao                    = "panico.alertas.auditoria"
	RoutingKeyAuditoriaPadrao              = "#"
	FilaDLQPadrao                          = "panico.alertas.dlq"
	TimeoutWorkerPadrao                    = 5 * time.Second
	URLBancoPadrao                         = "file:botao_panico.db"
	URLAPIExternaPadrao                    = "https://nominatim.openstreetmap.org"
	TimeoutAPIExternaPadrao                = 3 * time.Second
	AgentePadrao                           = "botao-panico-plataforma-mulher/1.0 (trabalho academico CIT7219)"
	TimeoutNotificacoesPadrao              = 5 * time.Second
	MinimoCaracteresChave                  = 16
	SMTPPortaPadrao                        = "587"
	CircuitBreakerMaxFalhasPadrao          = 3
	CircuitBreakerTimeoutRecuperacaoPadrao = 10 * time.Second
	NominatimIntervaloMinimoPadrao         = 1 * time.Second
)

// Carregar lê as variáveis usando os.LookupEnv.
func Carregar() (Config, error) {
	return CarregarDe(os.LookupEnv)
}

// CarregarDe permite injetar a fonte das variáveis (facilita os testes).
func CarregarDe(buscar func(string) (string, bool)) (Config, error) {
	ler := func(chave, padrao string) string {
		if v, ok := buscar(chave); ok {
			return strings.TrimSpace(v)
		}
		return padrao
	}

	var errs []error
	cfg := Config{
		Porta:              ler("PORT", PortaPadrao),
		PortaGRPC:          ler("GRPC_PORT", PortaGRPCPadrao),
		HostGRPC:           ler("GRPC_HOST", HostGRPCPadrao),
		URLBanco:           ler("DATABASE_URL", URLBancoPadrao),
		URLAPIExterna:      ler("EXTERNAL_API_URL", URLAPIExternaPadrao),
		AgenteAPIExterna:   ler("EXTERNAL_API_USER_AGENT", AgentePadrao),
		ChaveServico:       ler("SERVICE_KEY", ""),
		ExigirAutenticacao: lerBool(ler("EXIGIR_AUTENTICACAO", "false")),

		// RabbitMQ
		FilasAtivas:            lerBool(ler("FILAS_ATIVAS", "true")),
		AMQPURL:                ler("AMQP_URL", AMQPURLPadrao),
		ExchangeEventos:        ler("RABBITMQ_EXCHANGE", ExchangeEventosPadrao),
		ExchangeDLX:            ler("RABBITMQ_DLX", ExchangeDLXPadrao),
		FilaNotificacoes:       ler("RABBITMQ_QUEUE_NOTIFICACOES", FilaNotificacoesPadrao),
		RoutingKeyNotificacoes: ler("RABBITMQ_ROUTING_KEY_NOTIFICACOES", RoutingKeyNotificacoesPadrao),
		FilaAuditoria:          ler("RABBITMQ_QUEUE_AUDITORIA", FilaAuditoriaPadrao),
		RoutingKeyAuditoria:    ler("RABBITMQ_ROUTING_KEY_AUDITORIA", RoutingKeyAuditoriaPadrao),
		FilaDLQ:                ler("RABBITMQ_QUEUE_DLQ", FilaDLQPadrao),

		// E-mail demonstrativo acadêmico
		EmailDemoAtivo: lerBool(ler("EMAIL_DEMO_ATIVO", "false")),
		SMTPHost:       ler("SMTP_HOST", ""),
		SMTPPorta:      ler("SMTP_PORT", SMTPPortaPadrao),
		SMTPUsuario:    ler("SMTP_USER", ""),
		SMTPSenha:      ler("SMTP_PASS", ""),
		SMTPRemetente:  ler("SMTP_FROM", ""),
	}

	if timeoutWorker, err := lerDuracao(ler("WORKER_SHUTDOWN_TIMEOUT", ""), TimeoutWorkerPadrao); err == nil {
		cfg.TimeoutWorker = timeoutWorker
	} else {
		errs = append(errs, fmt.Errorf("WORKER_SHUTDOWN_TIMEOUT: %w", err))
	}

	if p, err := strconv.Atoi(cfg.Porta); err != nil || p < 1 || p > 65535 {
		errs = append(errs, fmt.Errorf("PORT inválida %q: use um número entre 1 e 65535", cfg.Porta))
	}
	if p, err := strconv.Atoi(cfg.PortaGRPC); err != nil || p < 1 || p > 65535 {
		errs = append(errs, fmt.Errorf("GRPC_PORT inválida %q: use um número entre 1 e 65535", cfg.PortaGRPC))
	}
	if cfg.Porta == cfg.PortaGRPC {
		errs = append(errs, fmt.Errorf("PORT e GRPC_PORT não podem usar a mesma porta (%s)", cfg.Porta))
	}
	if cfg.HostGRPC == "" {
		cfg.HostGRPC = HostGRPCPadrao
	}
	if cfg.URLBanco == "" {
		errs = append(errs, errors.New("DATABASE_URL não pode ser vazia"))
	}
	if cfg.URLAPIExterna != "" {
		u, err := url.Parse(cfg.URLAPIExterna)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			errs = append(errs, fmt.Errorf("EXTERNAL_API_URL inválida %q: use http(s)://host", cfg.URLAPIExterna))
		}
		if cfg.AgenteAPIExterna == "" {
			errs = append(errs, errors.New("EXTERNAL_API_USER_AGENT não pode ser vazio"))
		}
	}

	var err error
	if cfg.TimeoutAPIExterna, err = lerDuracao(ler("EXTERNAL_API_TIMEOUT", ""), TimeoutAPIExternaPadrao); err != nil {
		errs = append(errs, fmt.Errorf("EXTERNAL_API_TIMEOUT: %w", err))
	}
	if cfg.TimeoutNotificacoes, err = lerDuracao(ler("NOTIFICATION_TIMEOUT", ""), TimeoutNotificacoesPadrao); err != nil {
		errs = append(errs, fmt.Errorf("NOTIFICATION_TIMEOUT: %w", err))
	}

	// Resiliência / Circuit Breaker
	if falhas, err := strconv.Atoi(ler("CIRCUIT_BREAKER_MAX_FALHAS", strconv.Itoa(CircuitBreakerMaxFalhasPadrao))); err == nil && falhas > 0 {
		cfg.CircuitBreakerMaxFalhas = falhas
	} else if err != nil {
		errs = append(errs, fmt.Errorf("CIRCUIT_BREAKER_MAX_FALHAS inválido: %w", err))
	} else {
		cfg.CircuitBreakerMaxFalhas = CircuitBreakerMaxFalhasPadrao
	}

	if timeoutCB, err := lerDuracao(ler("CIRCUIT_BREAKER_TIMEOUT_RECUPERACAO", ""), CircuitBreakerTimeoutRecuperacaoPadrao); err == nil {
		cfg.CircuitBreakerTimeoutRecuperacao = timeoutCB
	} else {
		errs = append(errs, fmt.Errorf("CIRCUIT_BREAKER_TIMEOUT_RECUPERACAO: %w", err))
	}

	if intervalo, err := lerDuracaoIntervalo(ler("NOMINATIM_INTERVALO_MINIMO", ""), NominatimIntervaloMinimoPadrao); err == nil {
		cfg.NominatimIntervaloMinimo = intervalo
	} else {
		errs = append(errs, fmt.Errorf("NOMINATIM_INTERVALO_MINIMO: %w", err))
	}

	// Segurança: se a autenticação estiver ativada explicitamente ou se SERVICE_KEY for informada,
	// valida para impedir que falhas de configuração exponham endpoints sensíveis por engano.
	if cfg.ExigirAutenticacao || cfg.ChaveServico != "" {
		if cfg.ChaveServico == "" {
			errs = append(errs, errors.New("EXIGIR_AUTENTICACAO está ativa, mas SERVICE_KEY está vazia: o servidor não pode iniciar sem credencial válida"))
		} else if len(cfg.ChaveServico) < MinimoCaracteresChave {
			errs = append(errs, fmt.Errorf("SERVICE_KEY insegura: requer no mínimo %d caracteres (possui %d)", MinimoCaracteresChave, len(cfg.ChaveServico)))
		}
	}

	// Validação das credenciais do Modo B de e-mail se ativado
	if cfg.EmailDemoAtivo {
		if cfg.SMTPHost == "" {
			errs = append(errs, errors.New("EMAIL_DEMO_ATIVO está ativo, mas SMTP_HOST não foi configurado"))
		}
		if cfg.SMTPRemetente == "" {
			errs = append(errs, errors.New("EMAIL_DEMO_ATIVO está ativo, mas SMTP_FROM não foi configurado"))
		}
	}

	if len(errs) > 0 {
		return Config{}, errors.Join(errs...)
	}
	return cfg, nil
}

// GeocodificacaoAtiva indica se a API externa está configurada.
func (c Config) GeocodificacaoAtiva() bool {
	return c.URLAPIExterna != ""
}

// AutenticacaoAtiva indica se a autenticação serviço-a-serviço está configurada.
func (c Config) AutenticacaoAtiva() bool {
	return c.ChaveServico != ""
}

// EnderecoGRPC devolve o host:porta formatado para o listener gRPC.
func (c Config) EnderecoGRPC() string {
	return c.HostGRPC + ":" + c.PortaGRPC
}

func lerDuracao(valor string, padrao time.Duration) (time.Duration, error) {
	if valor == "" {
		return padrao, nil
	}
	d, err := time.ParseDuration(valor)
	if err != nil {
		return 0, fmt.Errorf("valor %q inválido (exemplos: 3s, 500ms)", valor)
	}
	if d <= 0 || d > time.Minute {
		return 0, fmt.Errorf("valor %q fora do intervalo (0, 1m]", valor)
	}
	return d, nil
}

func lerDuracaoIntervalo(valor string, padrao time.Duration) (time.Duration, error) {
	if valor == "" {
		return padrao, nil
	}
	if valor == "0" || valor == "0s" {
		return 0, nil
	}
	d, err := time.ParseDuration(valor)
	if err != nil {
		return 0, fmt.Errorf("valor %q inválido (exemplos: 1s, 500ms, 0s)", valor)
	}
	if d < 0 || d > time.Minute {
		return 0, fmt.Errorf("valor %q fora do intervalo [0, 1m]", valor)
	}
	return d, nil
}

func lerBool(valor string) bool {
	b, _ := strconv.ParseBool(strings.TrimSpace(valor))
	return b
}
