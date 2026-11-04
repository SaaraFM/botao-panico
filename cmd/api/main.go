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

	// 5. Camada HTTP REST (portas e endpoints para frontend e Edge Function).
	servidorHTTP := &http.Server{
		Addr:              ":" + cfg.Porta,
		Handler:           httpapi.NovoServidor(servico, cfg.ChaveServico).Rotas(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      30 * time.Second,
	}

	// 6. Camada gRPC (comunicação entre microserviços com Protocol Buffers).
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
		)
		if err := servidorGRPC.Serve(listenerGRPC); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			erroServidor <- err
		}
	}()

	select {
	case err := <-erroServidor:
		return err
	case <-ctx.Done():
		slog.Info("encerrando servidores REST e gRPC...")
		ctxDesligar, cancelarDesligar := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancelarDesligar()

		// Encerramento gracioso gRPC
		servidorGRPC.GracefulStop()

		// Encerramento gracioso HTTP
		return servidorHTTP.Shutdown(ctxDesligar)
	}
}
