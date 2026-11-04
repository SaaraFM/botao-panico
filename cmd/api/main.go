// Botão de Pânico — microserviço da Plataforma da Mulher.
//
// Recebe o acionamento do botão de pânico pela usuária, grava o alerta no
// banco SQLite próprio e notifica, em paralelo, seus contatos de emergência e
// — em risco alto/crítico — a rede de proteção de Araranguá/SC. Em paralelo,
// consulta a API Nominatim para registrar o endereço aproximado do alerta.
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
	"google.golang.org/grpc"
)

func main() {
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

	ctx, parar := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer parar()

	// 1. Persistência própria (SQLite; tabelas criadas automaticamente com migração idempotente).
	ctxBanco, cancelar := context.WithTimeout(ctx, 10*time.Second)
	repo, err := armazenamento.AbrirSQLite(ctxBanco, cfg.URLBanco)
	cancelar()
	if err != nil {
		return err
	}
	defer func() {
		if err := repo.Fechar(); err != nil {
			slog.Error("falha ao fechar o banco", "erro", err)
		}
	}()

	// 2. Notificações em paralelo (goroutines + WaitGroup + Mutex).
	despachante := notificacao.NovoDespachante(notificacao.SMSNotificador{}, notificacao.EmailNotificador{})

	// 3. API externa de geocodificação (opcional).
	var geo alerta.Geocodificador
	if cfg.GeocodificacaoAtiva() {
		geo = cliente.NovoNominatim(cfg.URLAPIExterna, cfg.AgenteAPIExterna, cfg.TimeoutAPIExterna)
	}

	// 4. Regras de negócio compartilhadas entre REST e gRPC.
	servico := alerta.NovoServico(repo, despachante, geo, cfg.TimeoutNotificacoes)

	// 5. Mensageria NATS JetStream e Transactional Outbox (Semana 12 CIT7219)
	var gerenciadorNATS *fila.GerenciadorNATS
	var relayOutbox *fila.OutboxRelay
	var workerNotificacoes *fila.ConsumidorNotificacoes

	if cfg.FilasAtivas {
		var errNATS error
		gerenciadorNATS, errNATS = fila.NovoGerenciadorNATS(fila.OpcoesNATS{
			URL:       cfg.URLNATS,
			Stream:    cfg.StreamNATS,
			Subject:   cfg.SubjectNATS,
			Consumer:  cfg.ConsumerNATS,
			Diretorio: cfg.DiretorioNATS,
		})
		if errNATS != nil {
			return fmt.Errorf("inicializar nats jetstream: %w", errNATS)
		}

		relayOutbox = fila.NovoOutboxRelay(repo, gerenciadorNATS.JS, cfg.SubjectNATS, 200*time.Millisecond)
		relayOutbox.Iniciar()

		workerNotificacoes = fila.NovoConsumidorNotificacoes(
			gerenciadorNATS.JS,
			servico,
			repo,
			fila.OpcoesConsumidor{
				Stream:     cfg.StreamNATS,
				Subject:    cfg.SubjectNATS,
				Consumer:   cfg.ConsumerNATS,
				GrupoFila:  "workers-notificacoes",
				NumWorkers: 3,
			},
		)
		if err := workerNotificacoes.Iniciar(); err != nil {
			return fmt.Errorf("iniciar worker de notificações: %w", err)
		}

		servico.AtivarModoAssincrono(relayOutbox.NotificarNovoAlerta)
		slog.Info("Processamento assíncrono NATS JetStream ativo",
			"stream", cfg.StreamNATS,
			"subject", cfg.SubjectNATS,
			"consumer", cfg.ConsumerNATS,
		)
	}

	// 6. Camada HTTP REST (portas e endpoints para frontend e Edge Function).
	servidorHTTP := &http.Server{
		Addr:              ":" + cfg.Porta,
		Handler:           httpapi.NovoServidor(servico, cfg.ChaveServico).Rotas(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      30 * time.Second,
	}

	// 7. Camada gRPC (comunicação entre microserviços com Protocol Buffers).
	var grpcOpts []grpc.ServerOption
	if cfg.AutenticacaoAtiva() {
		grpcOpts = append(grpcOpts,
			grpc.UnaryInterceptor(grpcapi.AutenticarUnary(cfg.ChaveServico)),
			grpc.StreamInterceptor(grpcapi.AutenticarStream(cfg.ChaveServico)),
		)
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
		slog.Info("Botão de Pânico REST ouvindo",
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
		slog.Info("Botão de Pânico gRPC ouvindo",
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
		slog.Info("encerrando servidores REST, gRPC e mensageria...")
		ctxDesligar, cancelarDesligar := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancelarDesligar()

		// Encerramento gracioso gRPC
		servidorGRPC.GracefulStop()

		// Encerramento gracioso HTTP
		errHTTP := servidorHTTP.Shutdown(ctxDesligar)

		// Encerramento Outbox Relay e Workers
		if relayOutbox != nil {
			relayOutbox.Parar()
		}
		if workerNotificacoes != nil {
			workerNotificacoes.Parar(cfg.TimeoutWorker)
		}
		if gerenciadorNATS != nil {
			gerenciadorNATS.Fechar()
		}
		return errHTTP
	}
}
