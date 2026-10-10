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
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/plataforma-mulher/botao-panico/internal/alerta"
	"github.com/plataforma-mulher/botao-panico/internal/armazenamento"
	"github.com/plataforma-mulher/botao-panico/internal/cliente"
	"github.com/plataforma-mulher/botao-panico/internal/config"
	"github.com/plataforma-mulher/botao-panico/internal/httpapi"
	"github.com/plataforma-mulher/botao-panico/internal/notificacao"
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

	// 1. Persistência própria (SQLite; tabelas criadas automaticamente).
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

	// 4. Regras de negócio e 5. camada HTTP.
	servico := alerta.NovoServico(repo, despachante, geo, cfg.TimeoutNotificacoes)
	servidor := &http.Server{
		Addr:              ":" + cfg.Porta,
		Handler:           httpapi.NovoServidor(servico).Rotas(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      30 * time.Second,
	}

	erroServidor := make(chan error, 1)
	go func() {
		slog.Info("Botão de Pânico ouvindo", "endereco", "http://localhost:"+cfg.Porta,
			"banco", cfg.URLBanco, "geocodificacao", cfg.GeocodificacaoAtiva())
		erroServidor <- servidor.ListenAndServe()
	}()

	select {
	case err := <-erroServidor:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	case <-ctx.Done():
		slog.Info("encerrando o servidor...")
		ctxDesligar, cancelarDesligar := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancelarDesligar()
		return servidor.Shutdown(ctxDesligar)
	}
}
