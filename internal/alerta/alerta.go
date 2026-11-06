// Package alerta contém as estruturas de domínio do microserviço de botão de pânico
// da Plataforma da Mulher. Aqui ficam os tipos centrais (structs) e suas regras.
package alerta

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"
)

// NivelRisco representa a gravidade da situação informada pela usuária.
type NivelRisco string

const (
	RiscoBaixo    NivelRisco = "baixo"
	RiscoModerado NivelRisco = "moderado"
	RiscoAlto     NivelRisco = "alto"
	RiscoCritico  NivelRisco = "critico"
)

// StatusAlerta representa o ciclo de vida de um alerta de pânico.
type StatusAlerta string

const (
	StatusCriado      StatusAlerta = "criado"
	StatusRecebido    StatusAlerta = "recebido"
	StatusProcessando StatusAlerta = "processando"
	StatusNotificando StatusAlerta = "notificando"
	StatusConcluido   StatusAlerta = "concluido"
	StatusFalha       StatusAlerta = "falha"
)

// Localizacao representa a posição geográfica da usuária no momento do alerta.
type Localizacao struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

// ContatoEmergencia é um contato de confiança cadastrado pela usuária
// (familiares, amigas) que será notificado quando o botão de pânico for acionado.
type ContatoEmergencia struct {
	ID       string `json:"id"`
	Nome     string `json:"nome"`
	Telefone string `json:"telefone"`
	Email    string `json:"email"`
}

// InstituicaoApoio é um órgão da rede de proteção (Polícia Militar, Delegacia
// da Mulher, CRAS etc.) que também pode receber alertas de risco alto/crítico.
type InstituicaoApoio struct {
	ID       string      `json:"id"`
	Nome     string      `json:"nome"`
	Tipo     string      `json:"tipo"` // ex.: "policia", "delegacia_mulher", "saude", "assistencia_social"
	Telefone string      `json:"telefone"`
	Local    Localizacao `json:"local"`
}

// Destinatario é quem recebe uma notificação do alerta
// (contato pessoal da usuária ou instituição de proteção).
type Destinatario struct {
	Nome     string
	Telefone string
	Email    string
}

// PedidoAlerta é o payload recebido pela API quando a usuária aciona o botão de pânico.
type PedidoAlerta struct {
	ChaveIdempotencia string      `json:"chave_idempotencia,omitempty"`
	UsuariaID         string      `json:"usuaria_id"`
	Nivel             NivelRisco  `json:"nivel"`
	Mensagem          string      `json:"mensagem"`
	Local             Localizacao `json:"local"`
	// Silencioso registra uma preferência para a interface/aparelho. No serviço
	// atual, não altera destinatários nem o despacho de notificações.
	Silencioso bool `json:"silencioso"`
}

// HashConteudo gera um resumo criptográfico dos dados do pedido para garantir
// que requisições repetidas com a mesma chave tenham exatamente o mesmo conteúdo.
func (p PedidoAlerta) HashConteudo() string {
	dados := fmt.Sprintf("%s|%s|%s|%.6f|%.6f|%t",
		p.UsuariaID, p.Nivel, p.Mensagem,
		p.Local.Latitude, p.Local.Longitude, p.Silencioso)
	h := sha256.Sum256([]byte(dados))
	return hex.EncodeToString(h[:])
}

// Alerta é o registro completo de um acionamento do botão de pânico.
//
// Local (latitude/longitude) é sempre a informação principal. EnderecoAproximado
// é complementar: vem da API externa de geocodificação e fica vazio se ela falhar.
type Alerta struct {
	ID                 string      `json:"id"`
	ChaveIdempotencia  string      `json:"chave_idempotencia,omitempty"`
	HashRequisicao     string      `json:"hash_requisicao,omitempty"`
	UsuariaID          string      `json:"usuaria_id"`
	Nivel              NivelRisco  `json:"nivel"`
	Mensagem           string      `json:"mensagem"`
	Local              Localizacao `json:"local"`
	EnderecoAproximado string      `json:"endereco_aproximado,omitempty"`
	// Silencioso preserva a preferência recebida; não suprime notificações.
	Silencioso       bool               `json:"silencioso"`
	Status           StatusAlerta       `json:"status"`
	CriadoEm         time.Time          `json:"criado_em"`
	AtualizadoEm     time.Time          `json:"atualizado_em"`
	Notificacoes     []ResultadoEnvio   `json:"notificacoes"`
	InstituicoesAlvo []InstituicaoApoio `json:"instituicoes_alvo,omitempty"`
	// Replay indica se o alerta foi recuperado por idempotência sem novo processamento.
	Replay bool `json:"-"`
}

// Copia devolve uma cópia profunda do alerta (slices inclusive), para que
// quem recebe o valor não compartilhe memória mutável com outras goroutines.
func (a Alerta) Copia() Alerta {
	c := a
	if a.Notificacoes != nil {
		c.Notificacoes = make([]ResultadoEnvio, len(a.Notificacoes))
		copy(c.Notificacoes, a.Notificacoes)
	}
	if a.InstituicoesAlvo != nil {
		c.InstituicoesAlvo = make([]InstituicaoApoio, len(a.InstituicoesAlvo))
		copy(c.InstituicoesAlvo, a.InstituicoesAlvo)
	}
	return c
}

// ResultadoEnvio registra o desfecho de cada tentativa de notificação
// enviada em paralelo pelo despachante.
type ResultadoEnvio struct {
	Destino   string `json:"destino"` // nome do contato ou da instituição
	Canal     string `json:"canal"`   // "sms" ou "email"
	Sucesso   bool   `json:"sucesso"`
	Erro      string `json:"erro,omitempty"`
	DuracaoMs int64  `json:"duracao_ms"`
	EnviadoEm string `json:"enviado_em"`
}

// RegistroAuditoria documenta de forma imutável a recepção de eventos para fins de auditoria e compliance.
type RegistroAuditoria struct {
	ID         string    `json:"id"`
	Evento     string    `json:"evento"`
	Versao     int       `json:"versao"`
	IDEvento   string    `json:"id_evento"`
	AlertaID   string    `json:"alerta_id"`
	RoutingKey string    `json:"routing_key"`
	RecebidoEm time.Time `json:"recebido_em"`
}
