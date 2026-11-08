package alerta

import (
	"time"
)

// EstadoTarefa define o ciclo de vida de uma tarefa no Transactional Outbox.
type EstadoTarefa string

const (
	EstadoTarefaPendente  EstadoTarefa = "pendente"
	EstadoTarefaPublicado EstadoTarefa = "publicado"
	EstadoTarefaConcluido EstadoTarefa = "concluido"
	EstadoTarefaFalha     EstadoTarefa = "falha"
)

// TarefaOutbox representa uma mensagem persistida atomicamente no SQLite antes de ser enviada ao broker.
type TarefaOutbox struct {
	ID                 string       `json:"id"`
	AlertaID           string       `json:"alerta_id"`
	Topico             string       `json:"topico"`
	PayloadJSON        string       `json:"payload_json"`
	Estado             EstadoTarefa `json:"estado"`
	Tentativas         int          `json:"tentativas"`
	MaxTentativas      int          `json:"max_tentativas"`
	ProximaTentativaEm time.Time    `json:"proxima_tentativa_em"`
	ErroUltimo         string       `json:"erro_ultimo,omitempty"`
	CriadoEm           time.Time    `json:"criado_em"`
	AtualizadoEm       time.Time    `json:"atualizado_em"`
}

// Envelope segue a especificação padronizada de eventos (baseada em EVENTOS.md).
// Garante interoperabilidade, versionamento e rastreabilidade nos brokers AMQP.
type Envelope struct {
	Evento        string      `json:"evento"`
	Versao        int         `json:"versao"`
	IDEvento      string      `json:"id_evento"`
	CorrelationID string      `json:"correlation_id,omitempty"` // Rastreamento ponta a ponta
	EmitidoEm     time.Time   `json:"emitido_em"`
	Dados         DadosEvento `json:"dados"`
}

// DadosEvento contém apenas identificadores e dados mínimos necessários para processamento (Data Minimization).
// Não expõe coordenadas sensíveis, dados pessoais ou mensagens de socorro na fila.
type DadosEvento struct {
	TarefaID  string `json:"tarefa_id"`
	AlertaID  string `json:"alerta_id"`
	UsuariaID string `json:"usuaria_id"`
	Nivel     string `json:"nivel"`
}
