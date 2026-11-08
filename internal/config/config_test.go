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
	assert.True(t, cfg.FilasAtivas)
	assert.Equal(t, "amqp://guest:guest@127.0.0.1:5672/", cfg.AMQPURL)
	assert.Equal(t, "panico.eventos", cfg.ExchangeEventos)
	assert.Equal(t, "panico.eventos.dlx", cfg.ExchangeDLX)
	assert.Equal(t, "panico.alertas.notificacoes", cfg.FilaNotificacoes)
	assert.Equal(t, "alerta.criado", cfg.RoutingKeyNotificacoes)
	assert.Equal(t, "panico.alertas.auditoria", cfg.FilaAuditoria)
	assert.Equal(t, "#", cfg.RoutingKeyAuditoria)
	assert.Equal(t, "panico.alertas.dlq", cfg.FilaDLQ)
	assert.Equal(t, 5*time.Second, cfg.TimeoutWorker)
	assert.False(t, cfg.EmailDemoAtivo)

	// Defaults de resiliência e rate limiting
	assert.Equal(t, 3, cfg.CircuitBreakerMaxFalhas)
	assert.Equal(t, 10*time.Second, cfg.CircuitBreakerTimeoutRecuperacao)
	assert.Equal(t, 1*time.Second, cfg.NominatimIntervaloMinimo)
	assert.True(t, cfg.MetricasAtivas)
	assert.True(t, cfg.MetricasAcessoLocalIrrestrito)
}

func TestCarregar_Personalizado(t *testing.T) {
	cfg, err := config.CarregarDe(fonte(map[string]string{
		"PORT": "9000", "GRPC_PORT": "9001", "GRPC_HOST": "127.0.0.1",
		"DATABASE_URL": "file:/tmp/x.db", "EXTERNAL_API_URL": "",
		"EXTERNAL_API_TIMEOUT": "750ms", "NOTIFICATION_TIMEOUT": "2s",
		"SERVICE_KEY":  "segredo-super-seguro-com-mais-de-16-chars",
		"FILAS_ATIVAS": "false",
		"CIRCUIT_BREAKER_MAX_FALHAS":          "5",
		"CIRCUIT_BREAKER_TIMEOUT_RECUPERACAO": "20s",
		"NOMINATIM_INTERVALO_MINIMO":         "500ms",
		"METRICAS_ATIVAS":                    "false",
		"METRICAS_ACESSO_LOCAL_IRRESTRITO":   "false",
	}))
	require.NoError(t, err)
	assert.False(t, cfg.FilasAtivas)
	assert.Equal(t, "9000", cfg.Porta)
	assert.Equal(t, "9001", cfg.PortaGRPC)
	assert.Equal(t, "127.0.0.1:9001", cfg.EnderecoGRPC())
	assert.Equal(t, "file:/tmp/x.db", cfg.URLBanco)
	assert.False(t, cfg.GeocodificacaoAtiva(), "URL vazia desativa a geocodificação")
	assert.Equal(t, 750*time.Millisecond, cfg.TimeoutAPIExterna)
	assert.Equal(t, 2*time.Second, cfg.TimeoutNotificacoes)
	assert.True(t, cfg.AutenticacaoAtiva())
	assert.Equal(t, "segredo-super-seguro-com-mais-de-16-chars", cfg.ChaveServico)
	assert.Equal(t, 5, cfg.CircuitBreakerMaxFalhas)
	assert.Equal(t, 20*time.Second, cfg.CircuitBreakerTimeoutRecuperacao)
	assert.Equal(t, 500*time.Millisecond, cfg.NominatimIntervaloMinimo)
	assert.False(t, cfg.MetricasAtivas)
	assert.False(t, cfg.MetricasAcessoLocalIrrestrito)
}

func TestCarregar_ConflitoPortas(t *testing.T) {
	_, err := config.CarregarDe(fonte(map[string]string{
		"PORT":      "8080",
		"GRPC_PORT": "8080",
	}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "não podem usar a mesma porta")
}

func TestCarregar_SegurancaAutenticacao(t *testing.T) {
	_, err := config.CarregarDe(fonte(map[string]string{
		"EXIGIR_AUTENTICACAO": "true",
	}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "EXIGIR_AUTENTICACAO")

	_, err = config.CarregarDe(fonte(map[string]string{
		"SERVICE_KEY": "chave-curta",
	}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "SERVICE_KEY insegura")
}

func TestCarregar_FilasMensageriaRabbitMQ(t *testing.T) {
	cfg, err := config.CarregarDe(fonte(map[string]string{
		"FILAS_ATIVAS":                      "true",
		"AMQP_URL":                          "amqp://usuario:senha@10.0.0.2:5672/",
		"RABBITMQ_EXCHANGE":                 "custom.eventos",
		"RABBITMQ_DLX":                      "custom.dlx",
		"RABBITMQ_QUEUE_NOTIFICACOES":       "custom.fila.notificacoes",
		"RABBITMQ_ROUTING_KEY_NOTIFICACOES": "evento.emergencia",
		"RABBITMQ_QUEUE_AUDITORIA":          "custom.fila.auditoria",
		"RABBITMQ_ROUTING_KEY_AUDITORIA":    "evento.*",
		"RABBITMQ_QUEUE_DLQ":                "custom.fila.dlq",
		"WORKER_SHUTDOWN_TIMEOUT":           "8s",
	}))
	require.NoError(t, err)
	assert.True(t, cfg.FilasAtivas)
	assert.Equal(t, "amqp://usuario:senha@10.0.0.2:5672/", cfg.AMQPURL)
	assert.Equal(t, "custom.eventos", cfg.ExchangeEventos)
	assert.Equal(t, "custom.dlx", cfg.ExchangeDLX)
	assert.Equal(t, "custom.fila.notificacoes", cfg.FilaNotificacoes)
	assert.Equal(t, "evento.emergencia", cfg.RoutingKeyNotificacoes)
	assert.Equal(t, "custom.fila.auditoria", cfg.FilaAuditoria)
	assert.Equal(t, "evento.*", cfg.RoutingKeyAuditoria)
	assert.Equal(t, "custom.fila.dlq", cfg.FilaDLQ)
	assert.Equal(t, 8*time.Second, cfg.TimeoutWorker)
}

func TestCarregar_EmailDemoValidacao(t *testing.T) {
	// Se EMAIL_DEMO_ATIVO for true mas SMTP não for informado, deve falhar
	_, err := config.CarregarDe(fonte(map[string]string{
		"EMAIL_DEMO_ATIVO": "true",
	}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "SMTP_HOST")

	// Com SMTP configurado corretamente
	cfg, err := config.CarregarDe(fonte(map[string]string{
		"EMAIL_DEMO_ATIVO": "true",
		"SMTP_HOST":        "smtp.gmail.com",
		"SMTP_FROM":        "teste@demo.local",
	}))
	require.NoError(t, err)
	assert.True(t, cfg.EmailDemoAtivo)
	assert.Equal(t, "smtp.gmail.com", cfg.SMTPHost)
	assert.Equal(t, "teste@demo.local", cfg.SMTPRemetente)
}

func TestCarregar_CircuitBreakerInvalido(t *testing.T) {
	_, err := config.CarregarDe(fonte(map[string]string{
		"CIRCUIT_BREAKER_MAX_FALHAS": "invalido",
	}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "CIRCUIT_BREAKER_MAX_FALHAS")

	_, err = config.CarregarDe(fonte(map[string]string{
		"CIRCUIT_BREAKER_TIMEOUT_RECUPERACAO": "tempo_errado",
	}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "CIRCUIT_BREAKER_TIMEOUT_RECUPERACAO")

	_, err = config.CarregarDe(fonte(map[string]string{
		"NOMINATIM_INTERVALO_MINIMO": "tempo_errado",
	}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "NOMINATIM_INTERVALO_MINIMO")
}
