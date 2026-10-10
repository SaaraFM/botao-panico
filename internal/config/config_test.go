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
	assert.Equal(t, config.URLBancoPadrao, cfg.URLBanco)
	assert.Equal(t, config.URLAPIExternaPadrao, cfg.URLAPIExterna)
	assert.Equal(t, 3*time.Second, cfg.TimeoutAPIExterna)
	assert.Equal(t, 5*time.Second, cfg.TimeoutNotificacoes)
	assert.True(t, cfg.GeocodificacaoAtiva())
}

func TestCarregar_Personalizado(t *testing.T) {
	cfg, err := config.CarregarDe(fonte(map[string]string{
		"PORT": "9000", "DATABASE_URL": "file:/tmp/x.db", "EXTERNAL_API_URL": "",
		"EXTERNAL_API_TIMEOUT": "750ms", "NOTIFICATION_TIMEOUT": "2s",
	}))
	require.NoError(t, err)
	assert.Equal(t, "9000", cfg.Porta)
	assert.Equal(t, "file:/tmp/x.db", cfg.URLBanco)
	assert.False(t, cfg.GeocodificacaoAtiva(), "URL vazia desativa a geocodificação")
	assert.Equal(t, 750*time.Millisecond, cfg.TimeoutAPIExterna)
	assert.Equal(t, 2*time.Second, cfg.TimeoutNotificacoes)
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
	}
	for nome, vars := range casos {
		t.Run(nome, func(t *testing.T) {
			_, err := config.CarregarDe(fonte(vars))
			assert.Error(t, err)
		})
	}
}
