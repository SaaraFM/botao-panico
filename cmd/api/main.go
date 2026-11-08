// Botão de Pânico — microserviço da Plataforma da Mulher.
//
// Recebe o acionamento do botão de pânico pela usuária, grava o alerta e a tarefa
// na mesma transação SQLite (Transactional Outbox) e publica eventos no RabbitMQ (AMQP 0-9-1).
// Dois consumidores independentes processam o mesmo evento em paralelo:
//   - Consumidor A: processa notificações simuladas e atualiza o status para concluído.
//   - Consumidor B: audita todos os eventos na fila panico.alertas.auditoria (binding #).
//
// Mensagens inválidas são automaticamente encaminhadas à Dead-Letter Queue (DLQ).
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/plataforma-mulher/botao-panico/internal/alerta"
	"github.com/plataforma-mulher/botao-panico/internal/armazenamento"
	"github.com/plataforma-mulher/botao-panico/internal/cliente"
	"github.com/plataforma-mulher/botao-panico/internal/config"
	"github.com/plataforma-mulher/botao-panico/internal/fila"
	"github.com/plataforma-mulher/botao-panico/internal/grpcapi"
	"github.com/plataforma-mulher/botao-panico/internal/httpapi"
	"github.com/plataforma-mulher/botao-panico/internal/notificacao"
	"github.com/plataforma-mulher/botao-panico/internal/observabilidade"
	"google.golang.org/grpc"
)

func main() {
	// Requisito 8 (Observabilidade): Configura o logger padrão da biblioteca oficial (log/slog)
	// para emitir logs estritamente no formato JSON estruturado na saída padrão (stdout).
	// Isso permite que coletores de logs e o terminal exibam chaves e valores padronizados
	// (ex: time, level, msg, rota, alerta_id, duracao_ms) sem ferramentas pesadas como Grafana.
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	if err := executar(); err != nil {
		slog.Error("microserviço encerrado com erro", "erro", err)
		os.Exit(1)
	}
}

func executar() error {
	cfg, err := config.Carregar()
	if err != nil {
		return err
	}

	// Graceful Shutdown: captura sinais do sistema operacional (Ctrl+C / SIGINT e SIGTERM)
	// criando um context.Context que é cancelado de forma ordenada ao receber o encerramento.
	ctx, parar := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer parar()

	slog.Info("iniciando microserviço do botão de pânico",
		"porta_rest", cfg.Porta,
		"porta_grpc", cfg.PortaGRPC,
		"filas_ativas", cfg.FilasAtivas,
		"banco", cfg.URLBanco,
	)

	// 1. Persistência própria (SQLite; migração idempotente com Transactional Outbox e Auditoria).
	ctxBanco, cancelarBanco := context.WithTimeout(ctx, 10*time.Second)
	repo, err := armazenamento.AbrirSQLite(ctxBanco, cfg.URLBanco)
	cancelarBanco()
	if err != nil {
		return err
	}
	defer func() {
		if err := repo.Fechar(); err != nil {
			slog.Error("falha ao fechar o banco", "erro", err)
		}
	}()

	// 2. Observabilidade avançada (Métricas Prometheus isoladas)
	var metricas *observabilidade.Metricas
	if cfg.MetricasAtivas {
		metricas = observabilidade.NovaMetricas()
	}

	// 2. Notificações em paralelo (goroutines + WaitGroup + Mutex).
	despachante := notificacao.NovoDespachante(notificacao.SMSNotificador{}, notificacao.EmailNotificador{})

	// 3. API externa de geocodificação (opcional).
	var geo alerta.Geocodificador
	if cfg.GeocodificacaoAtiva() {
		geo = cliente.NovoNominatimComOpcoes(cliente.OpcoesNominatim{
			URLBase:            cfg.URLAPIExterna,
			Agente:             cfg.AgenteAPIExterna,
			Timeout:            cfg.TimeoutAPIExterna,
			MaxFalhasCB:        cfg.CircuitBreakerMaxFalhas,
			TimeoutRecuperacao: cfg.CircuitBreakerTimeoutRecuperacao,
			IntervaloMinimo:    cfg.NominatimIntervaloMinimo,
			Metricas:           metricas,
		})
	}

	// 4. Regras de negócio compartilhadas entre REST e gRPC.
	servico := alerta.NovoServico(repo, despachante, geo, cfg.TimeoutNotificacoes)

	// 5. E-mail demonstrativo acadêmico (FASE 6 - restrito e desativado por padrão)
	var emailDemo *notificacao.EmailDemoNotificador
	if cfg.EmailDemoAtivo {
		emailDemo = notificacao.NovoEmailDemoNotificador(notificacao.ConfigEmailDemo{
			Ativo:     true,
			Host:      cfg.SMTPHost,
			Porta:     cfg.SMTPPorta,
			Usuario:   cfg.SMTPUsuario,
			Senha:     cfg.SMTPSenha,
			Remetente: cfg.SMTPRemetente,
		})
		slog.Info("e-mail demonstrativo real ATIVADO (restrito a "+config.EmailAutorizadoDemo+")",
			"destinatario_autorizado", config.EmailAutorizadoDemo,
			"smtp_host", cfg.SMTPHost,
		)
	}

	// 6. Mensageria RabbitMQ AMQP 0-9-1 (Semana 13 CIT7219)
	var gerRabbitMQ *fila.GerenciadorRabbitMQ
	var relayOutbox *fila.OutboxRelay
	var consNotificacoes *fila.ConsumidorNotificacoes
	var consAuditoria *fila.ConsumidorAuditoria

	if cfg.FilasAtivas {
		var errRabbit error
		gerRabbitMQ, errRabbit = fila.NovoGerenciadorRabbitMQ(fila.OpcoesRabbitMQ{
			URL:                    cfg.AMQPURL,
			ExchangeEventos:        cfg.ExchangeEventos,
			ExchangeDLX:            cfg.ExchangeDLX,
			FilaNotificacoes:       cfg.FilaNotificacoes,
			RoutingKeyNotificacoes: cfg.RoutingKeyNotificacoes,
			FilaAuditoria:          cfg.FilaAuditoria,
			RoutingKeyAuditoria:    cfg.RoutingKeyAuditoria,
			FilaDLQ:                cfg.FilaDLQ,
		})
		if errRabbit != nil {
			return fmt.Errorf("inicializar infraestrutura rabbitmq: %w", errRabbit)
		}

		relayOutbox = fila.NovoOutboxRelay(repo, gerRabbitMQ, cfg.RoutingKeyNotificacoes, 200*time.Millisecond)
		if metricas != nil {
			relayOutbox.ConectarMetricas(metricas)
		}
		relayOutbox.Iniciar()

		// Consumidor A: Notificações
		consNotificacoes, err = fila.NovoConsumidorNotificacoes(
			gerRabbitMQ.Conexao(),
			servico,
			repo,
			fila.OpcoesConsumidorNotificacoes{
				Fila:      cfg.FilaNotificacoes,
				EmailDemo: emailDemo,
				Metricas:  metricas,
			},
		)
		if err != nil {
			return fmt.Errorf("inicializar consumidor de notificações: %w", err)
		}
		if err := consNotificacoes.Iniciar(); err != nil {
			return fmt.Errorf("iniciar consumo de notificações: %w", err)
		}

		// Consumidor B: Auditoria Independente (binding #)
		consAuditoria, err = fila.NovoConsumidorAuditoria(
			gerRabbitMQ.Conexao(),
			servico,
			fila.OpcoesConsumidorAuditoria{
				Fila:     cfg.FilaAuditoria,
				Metricas: metricas,
			},
		)
		if err != nil {
			return fmt.Errorf("inicializar consumidor de auditoria: %w", err)
		}
		if err := consAuditoria.Iniciar(); err != nil {
			return fmt.Errorf("iniciar consumo de auditoria: %w", err)
		}

		servico.AtivarModoAssincrono(relayOutbox.NotificarNovoAlerta)
		slog.Info("processamento assíncrono RabbitMQ ativo (Pub/Sub com dois consumidores independentes)",
			"exchange", cfg.ExchangeEventos,
			"fila_notificacoes", cfg.FilaNotificacoes,
			"fila_auditoria", cfg.FilaAuditoria,
			"fila_dlq", cfg.FilaDLQ,
		)
	}

	// 7. Camada HTTP REST (portas e endpoints para frontend, Prometheus e Edge Function).
	verificadorSaude := func(ctx context.Context) map[string]any {
		comps := make(map[string]any)
		if err := repo.Ping(ctx); err != nil {
			comps["sqlite"] = map[string]any{"status": "erro", "detalhe": err.Error()}
		} else {
			comps["sqlite"] = map[string]any{"status": "ok"}
		}
		if cfg.FilasAtivas && gerRabbitMQ != nil {
			if gerRabbitMQ.Conexao() == nil || gerRabbitMQ.Conexao().IsClosed() {
				comps["rabbitmq"] = map[string]any{"status": "erro", "detalhe": "conexão amqp fechada"}
			} else {
				comps["rabbitmq"] = map[string]any{"status": "ok"}
			}
		}
		return comps
	}
	servidorHTTP := &http.Server{
		Addr:              ":" + cfg.Porta,
		Handler: httpapi.NovoServidorComOpcoes(servico, httpapi.OpcoesServidor{
			ChaveServico:                  cfg.ChaveServico,
			Metricas:                      metricas,
			MetricasAtivas:                cfg.MetricasAtivas,
			MetricasAcessoLocalIrrestrito: cfg.MetricasAcessoLocalIrrestrito,
			VerificadorSaude:              verificadorSaude,
		}).Rotas(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      30 * time.Second,
	}

	// 8. Camada gRPC com Protocol Buffers e observabilidade via interceptor.
	var unaryInterceptors []grpc.UnaryServerInterceptor
	unaryInterceptors = append(unaryInterceptors, grpcapi.LoggingUnary(metricas))
	if cfg.AutenticacaoAtiva() {
		unaryInterceptors = append(unaryInterceptors, grpcapi.AutenticarUnary(cfg.ChaveServico))
	}

	var grpcOpts []grpc.ServerOption
	grpcOpts = append(grpcOpts, grpc.ChainUnaryInterceptor(unaryInterceptors...))
	if cfg.AutenticacaoAtiva() {
		grpcOpts = append(grpcOpts, grpc.StreamInterceptor(grpcapi.AutenticarStream(cfg.ChaveServico)))
	}

	servidorGRPC := grpc.NewServer(grpcOpts...)
	servicoGRPC := grpcapi.NovoServidor(servico, cfg.ChaveServico)
	servicoGRPC.RegistrarServico(servidorGRPC)

	listenerGRPC, err := net.Listen("tcp", cfg.EnderecoGRPC())
	if err != nil {
		return err
	}
	defer listenerGRPC.Close()

	erroServidor := make(chan error, 2)

	// Inicia REST
	go func() {
		slog.Info("servidor REST ouvindo",
			"endereco", "http://localhost:"+cfg.Porta,
			"banco", cfg.URLBanco,
			"geocodificacao", cfg.GeocodificacaoAtiva(),
			"autenticacao", cfg.AutenticacaoAtiva(),
			"filas_ativas", cfg.FilasAtivas,
		)
		if err := servidorHTTP.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			erroServidor <- err
		}
	}()

	// Inicia gRPC (escutando em localhost/configurado)
	go func() {
		slog.Info("servidor gRPC ouvindo",
			"endereco", cfg.EnderecoGRPC(),
			"autenticacao", cfg.AutenticacaoAtiva(),
			"filas_ativas", cfg.FilasAtivas,
		)
		if err := servidorGRPC.Serve(listenerGRPC); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			erroServidor <- err
		}
	}()

	select {
	case err := <-erroServidor:
		return err
	case <-ctx.Done():
		slog.Info("encerrando servidores REST, gRPC e mensageria RabbitMQ...")
		ctxDesligar, cancelarDesligar := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancelarDesligar()

		// Encerramento gracioso gRPC
		servidorGRPC.GracefulStop()

		// Encerramento gracioso HTTP
		errHTTP := servidorHTTP.Shutdown(ctxDesligar)

		// Encerramento Outbox Relay e Consumidores
		if relayOutbox != nil {
			relayOutbox.Parar()
		}
		if consNotificacoes != nil {
			consNotificacoes.Parar(cfg.TimeoutWorker)
		}
		if consAuditoria != nil {
			consAuditoria.Parar(cfg.TimeoutWorker)
		}
		if gerRabbitMQ != nil {
			_ = gerRabbitMQ.Fechar()
		}

		slog.Info("microserviço encerrado graciosamente")
		return errHTTP
	}
}
