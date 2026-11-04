package config_test

import (
	"testing"
	"time"

	"github.com/plataforma-mulher/botao-panico/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func fonte(vars map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) {
		v, ok := vars[k]
		return v, ok
	}
}

func TestCarregar_Padroes(t *testing.T) {
	cfg, err := config.CarregarDe(fonte(nil))
	require.NoError(t, err)
	assert.Equal(t, "8080", cfg.Porta)
	assert.Equal(t, "50051", cfg.PortaGRPC)
	assert.Equal(t, "127.0.0.1", cfg.HostGRPC)
	assert.Equal(t, "127.0.0.1:50051", cfg.EnderecoGRPC())
	assert.Equal(t, config.URLBancoPadrao, cfg.URLBanco)
	assert.Equal(t, config.URLAPIExternaPadrao, cfg.URLAPIExterna)
	assert.Equal(t, 3*time.Second, cfg.TimeoutAPIExterna)
	assert.Equal(t, 5*time.Second, cfg.TimeoutNotificacoes)
	assert.True(t, cfg.GeocodificacaoAtiva())
	assert.False(t, cfg.AutenticacaoAtiva())
	assert.Empty(t, cfg.ChaveServico)
	assert.False(t, cfg.ExigirAutenticacao)
}

func TestCarregar_Personalizado(t *testing.T) {
	cfg, err := config.CarregarDe(fonte(map[string]string{
		"PORT": "9000", "GRPC_PORT": "9001", "GRPC_HOST": "127.0.0.1",
		"DATABASE_URL": "file:/tmp/x.db", "EXTERNAL_API_URL": "",
		"EXTERNAL_API_TIMEOUT": "750ms", "NOTIFICATION_TIMEOUT": "2s",
		"SERVICE_KEY": "segredo-super-seguro-com-mais-de-16-chars",
	}))
	require.NoError(t, err)
	assert.Equal(t, "9000", cfg.Porta)
	assert.Equal(t, "9001", cfg.PortaGRPC)
	assert.Equal(t, "127.0.0.1:9001", cfg.EnderecoGRPC())
	assert.Equal(t, "file:/tmp/x.db", cfg.URLBanco)
	assert.False(t, cfg.GeocodificacaoAtiva(), "URL vazia desativa a geocodificação")
	assert.Equal(t, 750*time.Millisecond, cfg.TimeoutAPIExterna)
	assert.Equal(t, 2*time.Second, cfg.TimeoutNotificacoes)
	assert.True(t, cfg.AutenticacaoAtiva())
	assert.Equal(t, "segredo-super-seguro-com-mais-de-16-chars", cfg.ChaveServico)
}

func TestCarregar_ConflitoPortas(t *testing.T) {
	// Mesma porta para REST e gRPC deve falhar
	_, err := config.CarregarDe(fonte(map[string]string{
		"PORT":      "8080",
		"GRPC_PORT": "8080",
	}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "não podem usar a mesma porta")
}

func TestCarregar_SegurancaAutenticacao(t *testing.T) {
	// EXIGIR_AUTENTICACAO=true sem SERVICE_KEY deve falhar
	_, err := config.CarregarDe(fonte(map[string]string{
		"EXIGIR_AUTENTICACAO": "true",
	}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "EXIGIR_AUTENTICACAO")

	// SERVICE_KEY muito curta deve falhar
	_, err = config.CarregarDe(fonte(map[string]string{
		"SERVICE_KEY": "chave-curta",
	}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "SERVICE_KEY insegura")
}

func TestCarregar_Invalidos(t *testing.T) {
	casos := map[string]map[string]string{
		"porta texto":      {"PORT": "abc"},
		"porta fora":       {"PORT": "70000"},
		"banco vazio":      {"DATABASE_URL": ""},
		"url sem esquema":  {"EXTERNAL_API_URL": "nominatim.org"},
		"timeout inválido": {"EXTERNAL_API_TIMEOUT": "rápido"},
		"timeout negativo": {"EXTERNAL_API_TIMEOUT": "-1s"},
		"timeout notif":    {"NOTIFICATION_TIMEOUT": "2h"},
		"agente vazio":     {"EXTERNAL_API_USER_AGENT": ""},
	}
	for nome, vars := range casos {
		t.Run(nome, func(t *testing.T) {
			_, err := config.CarregarDe(fonte(vars))
			assert.Error(t, err)
		})
	}
}
