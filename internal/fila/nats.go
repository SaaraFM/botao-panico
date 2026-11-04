package fila

import (
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// GerenciadorNATS encapsula a conexão NATS, o contexto JetStream e opcionalmente o servidor embutido.
type GerenciadorNATS struct {
	NC       *nats.Conn
	JS       nats.JetStreamContext
	Servidor *natsserver.Server
	Stream   string
	Subject  string
	Consumer string
}

// OpcoesNATS reúne parâmetros de inicialização do NATS JetStream.
type OpcoesNATS struct {
	URL          string        // URL externa (ex.: nats://127.0.0.1:4222); se vazio, inicia servidor embutido
	Stream       string        // Nome da stream (padrão: ALERTAS)
	Subject      string        // Subject para mensagens (padrão: alertas.notificar)
	Consumer     string        // Nome do consumer durável (padrão: worker-notificacoes)
	Diretorio    string        // Diretório para persistência em disco (padrão: data/jetstream)
	EmMemoria    bool          // Se true, utiliza armazenamento em memória (para testes rápidos)
	PortaInterna int           // Porta do servidor embutido (0 = aleatória)
}

// NovoGerenciadorNATS conecta ao broker externo ou inicializa um servidor JetStream embutido com persistência em disco.
func NovoGerenciadorNATS(opts OpcoesNATS) (*GerenciadorNATS, error) {
	if opts.Stream == "" {
		opts.Stream = "ALERTAS"
	}
	if opts.Subject == "" {
		opts.Subject = "alertas.notificar"
	}
	if opts.Consumer == "" {
		opts.Consumer = "worker-notificacoes"
	}

	var ns *natsserver.Server
	urlConexao := opts.URL

	// 1. Se URL não fornecida, inicia servidor JetStream embutido seguro em 127.0.0.1
	if urlConexao == "" {
		storeDir := opts.Diretorio
		if storeDir == "" && !opts.EmMemoria {
			storeDir = filepath.Join("data", "jetstream")
		}

		if storeDir != "" && !opts.EmMemoria {
			if err := os.MkdirAll(storeDir, 0750); err != nil {
				return nil, fmt.Errorf("criar diretorio nats store: %w", err)
			}
		}

		porta := opts.PortaInterna
		if porta == 0 {
			porta = 4222
			// Se 4222 estiver ocupada, escolhe porta dinâmica
			l, err := net.Listen("tcp", "127.0.0.1:4222")
			if err != nil {
				porta = -1 // NATS escolhe porta livre
			} else {
				_ = l.Close()
			}
		}

		serverOpts := &natsserver.Options{
			Host:      "127.0.0.1", // Restrito a localhost por segurança
			Port:      porta,
			JetStream: true,
			StoreDir:  storeDir,
			NoLog:     true,
			NoSigs:    true,
		}

		var err error
		ns, err = natsserver.NewServer(serverOpts)
		if err != nil {
			return nil, fmt.Errorf("criar nats server embutido: %w", err)
		}

		ns.Start()
		if !ns.ReadyForConnections(5 * time.Second) {
			ns.Shutdown()
			return nil, fmt.Errorf("tempo esgotado aguardando nats server iniciar")
		}

		urlConexao = ns.ClientURL()
		slog.Info("NATS JetStream embutido iniciado", "url", urlConexao, "storeDir", storeDir, "emMemoria", opts.EmMemoria)
	}

	// 2. Conexão do cliente com retentativas automáticas
	nc, err := nats.Connect(urlConexao,
		nats.Name("botao-panico-backend"),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(1*time.Second),
	)
	if err != nil {
		if ns != nil {
			ns.Shutdown()
		}
		return nil, fmt.Errorf("conectar nats (%s): %w", urlConexao, err)
	}

	// 3. Inicializa contexto JetStream
	js, err := nc.JetStream(nats.PublishAsyncMaxPending(256))
	if err != nil {
		nc.Close()
		if ns != nil {
			ns.Shutdown()
		}
		return nil, fmt.Errorf("iniciar jetstream: %w", err)
	}

	// 4. Cria ou atualiza a Stream persistente com deduplicação de mensagens
	storageType := nats.FileStorage
	if opts.EmMemoria {
		storageType = nats.MemoryStorage
	}

	streamConfig := &nats.StreamConfig{
		Name:        opts.Stream,
		Subjects:    []string{"alertas.>"},
		Storage:     storageType,
		Duplicates:  5 * time.Minute, // Janela de deduplicação nativa por Nats-Msg-Id
		Retention:   nats.LimitsPolicy,
		MaxAge:      24 * time.Hour,
		MaxMsgs:     100000,
	}

	_, err = js.AddStream(streamConfig)
	if err != nil {
		// Se já existir, atualiza
		_, err = js.UpdateStream(streamConfig)
		if err != nil {
			nc.Close()
			if ns != nil {
				ns.Shutdown()
			}
			return nil, fmt.Errorf("configurar stream nats %s: %w", opts.Stream, err)
		}
	}

	return &GerenciadorNATS{
		NC:       nc,
		JS:       js,
		Servidor: ns,
		Stream:   opts.Stream,
		Subject:  opts.Subject,
		Consumer: opts.Consumer,
	}, nil
}

// Fechar drena a conexão e desliga o servidor embutido se existente.
func (g *GerenciadorNATS) Fechar() {
	if g.NC != nil && !g.NC.IsClosed() {
		_ = g.NC.Drain()
		g.NC.Close()
	}
	if g.Servidor != nil {
		g.Servidor.Shutdown()
		g.Servidor.WaitForShutdown()
	}
}
