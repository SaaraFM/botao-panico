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
	URLBanco            string        // DATABASE_URL (DSN do SQLite)
	URLAPIExterna       string        // EXTERNAL_API_URL (vazio desativa a geocodificação)
	TimeoutAPIExterna   time.Duration // EXTERNAL_API_TIMEOUT
	AgenteAPIExterna    string        // EXTERNAL_API_USER_AGENT
	TimeoutNotificacoes time.Duration // NOTIFICATION_TIMEOUT
}

// Valores padrão para desenvolvimento local (sem nenhuma credencial).
const (
	PortaPadrao               = "8080"
	URLBancoPadrao            = "file:botao_panico.db"
	URLAPIExternaPadrao       = "https://nominatim.openstreetmap.org"
	TimeoutAPIExternaPadrao   = 3 * time.Second
	AgentePadrao              = "botao-panico-plataforma-mulher/1.0 (trabalho academico CIT7219)"
	TimeoutNotificacoesPadrao = 5 * time.Second
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
		Porta:            ler("PORT", PortaPadrao),
		URLBanco:         ler("DATABASE_URL", URLBancoPadrao),
		URLAPIExterna:    ler("EXTERNAL_API_URL", URLAPIExternaPadrao),
		AgenteAPIExterna: ler("EXTERNAL_API_USER_AGENT", AgentePadrao),
	}

	if p, err := strconv.Atoi(cfg.Porta); err != nil || p < 1 || p > 65535 {
		errs = append(errs, fmt.Errorf("PORT inválida %q: use um número entre 1 e 65535", cfg.Porta))
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

	if len(errs) > 0 {
		return Config{}, errors.Join(errs...)
	}
	return cfg, nil
}

// GeocodificacaoAtiva indica se a API externa está configurada.
func (c Config) GeocodificacaoAtiva() bool {
	return c.URLAPIExterna != ""
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
