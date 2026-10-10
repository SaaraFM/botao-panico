package httpapi

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	"github.com/plataforma-mulher/botao-panico/internal/alerta"
)

// Códigos estáveis do JSON de erro (o cliente pode tratar por código).
const (
	CodigoJSONInvalido      = "JSON_INVALIDO"
	CodigoCorpoGrande       = "CORPO_MUITO_GRANDE"
	CodigoDadosInvalidos    = "DADOS_INVALIDOS"
	CodigoNaoEncontrado     = "NAO_ENCONTRADO"
	CodigoBancoIndisponivel = "BANCO_INDISPONIVEL"
	CodigoTempoEsgotado     = "TEMPO_ESGOTADO"
	CodigoErroInterno       = "ERRO_INTERNO"
)

// RespostaErro é o formato padrão de erro: {"erro": "...", "codigo": "..."}.
// "campos" só aparece em erros de validação (contrato mantido da versão anterior).
type RespostaErro struct {
	Erro   string            `json:"erro"`
	Codigo string            `json:"codigo"`
	Campos map[string]string `json:"campos,omitempty"`
}

type erroHTTP struct {
	status int
	corpo  RespostaErro
}

func novoErro(status int, codigo, mensagem string) *erroHTTP {
	return &erroHTTP{status: status, corpo: RespostaErro{Erro: mensagem, Codigo: codigo}}
}

// traduzirErro mapeia erros de negócio e de infraestrutura para HTTP.
// Mensagens internas (SQL, rede) nunca vão para o cliente — só para o log.
func traduzirErro(err error) *erroHTTP {
	var validacao *alerta.ErrosValidacao
	switch {
	// --- erros de negócio ---
	case errors.As(err, &validacao):
		e := novoErro(http.StatusUnprocessableEntity, CodigoDadosInvalidos, "dados inválidos")
		e.corpo.Campos = validacao.Campos
		return e
	case errors.Is(err, alerta.ErrNaoEncontrado):
		return novoErro(http.StatusNotFound, CodigoNaoEncontrado, "alerta não encontrado")

	// --- erros de infraestrutura ---
	case errors.Is(err, alerta.ErrBancoIndisponivel):
		return novoErro(http.StatusServiceUnavailable, CodigoBancoIndisponivel, "serviço temporariamente indisponível")
	case errors.Is(err, context.DeadlineExceeded):
		return novoErro(http.StatusServiceUnavailable, CodigoTempoEsgotado, "tempo de processamento esgotado")
	default: // ErrEscrita, ErrLeitura ou falha inesperada
		return novoErro(http.StatusInternalServerError, CodigoErroInterno, "falha interna ao processar a requisição")
	}
}

// responderFalha registra o erro completo no log e envia a versão segura ao cliente.
func responderFalha(w http.ResponseWriter, err error) {
	e := traduzirErro(err)
	if e.status >= 500 {
		slog.Error("erro de infraestrutura", "status", e.status, "erro", err)
	}
	responderJSON(w, e.status, e.corpo)
}
