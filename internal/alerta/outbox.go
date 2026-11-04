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

// TarefaOutbox representa uma mensagem persistida atômica no SQLite antes de ser enviada ao broker.
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

// EventoAlertaCriado é o payload compacto trafegado na fila NATS JetStream.
// Aplica minimização de dados (PII): coordenadas e mensagens completas ficam no SQLite.
type EventoAlertaCriado struct {
	TarefaID  string `json:"tarefa_id"`
	AlertaID  string `json:"alerta_id"`
	UsuariaID string `json:"usuaria_id"`
	Nivel     string `json:"nivel"`
	CriadoEm  string `json:"criado_em"`
}
