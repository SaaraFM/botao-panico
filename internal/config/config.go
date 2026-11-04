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

	// Configurações de Mensageria NATS JetStream (Semana 12 CIT7219)
	FilasAtivas         bool          // FILAS_ATIVAS (habilita NATS e Outbox assíncrono)
	URLNATS             string        // NATS_URL (ex.: nats://127.0.0.1:4222; vazio inicia embutido com arquivo)
	StreamNATS          string        // NATS_STREAM (padrão: ALERTAS)
	SubjectNATS         string        // NATS_SUBJECT (padrão: alertas.notificar)
	ConsumerNATS        string        // NATS_CONSUMER (padrão: worker-notificacoes)
	DiretorioNATS       string        // NATS_STORE_DIR (padrão: data/jetstream para persistência em disco)
	TimeoutWorker       time.Duration // WORKER_SHUTDOWN_TIMEOUT
}

// Valores padrão para desenvolvimento local (sem nenhuma credencial).
const (
	PortaPadrao               = "8080"
	PortaIntegracao           = "8081" // Porta recomendada para integração com Plataforma da Mulher
	PortaGRPCPadrao           = "50051"
	HostGRPCPadrao            = "127.0.0.1" // Padrão seguro restrito a localhost
	StreamNATSPadrao          = "ALERTAS"
	SubjectNATSPadrao         = "alertas.notificar"
	ConsumerNATSPadrao        = "worker-notificacoes"
	DiretorioNATSPadrao       = "data/jetstream"
	TimeoutWorkerPadrao       = 5 * time.Second
	URLBancoPadrao            = "file:botao_panico.db"
	URLAPIExternaPadrao       = "https://nominatim.openstreetmap.org"
	TimeoutAPIExternaPadrao   = 3 * time.Second
	AgentePadrao              = "botao-panico-plataforma-mulher/1.0 (trabalho academico CIT7219)"
	TimeoutNotificacoesPadrao = 5 * time.Second
	MinimoCaracteresChave     = 16
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

		FilasAtivas:   lerBool(ler("FILAS_ATIVAS", "true")),
		URLNATS:       ler("NATS_URL", ""),
		StreamNATS:    ler("NATS_STREAM", StreamNATSPadrao),
		SubjectNATS:   ler("NATS_SUBJECT", SubjectNATSPadrao),
		ConsumerNATS:  ler("NATS_CONSUMER", ConsumerNATSPadrao),
		DiretorioNATS: ler("NATS_STORE_DIR", DiretorioNATSPadrao),
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

	// Segurança: se a autenticação estiver ativada explicitamente ou se SERVICE_KEY for informada,
	// valida para impedir que falhas de configuração exponham endpoints sensíveis por engano.
	if cfg.ExigirAutenticacao || cfg.ChaveServico != "" {
		if cfg.ChaveServico == "" {
			errs = append(errs, errors.New("EXIGIR_AUTENTICACAO está ativa, mas SERVICE_KEY está vazia: o servidor não pode iniciar sem credencial válida"))
		} else if len(cfg.ChaveServico) < MinimoCaracteresChave {
			errs = append(errs, fmt.Errorf("SERVICE_KEY insegura: requer no mínimo %d caracteres (possui %d)", MinimoCaracteresChave, len(cfg.ChaveServico)))
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

func lerBool(valor string) bool {
	b, _ := strconv.ParseBool(strings.TrimSpace(valor))
	return b
}
