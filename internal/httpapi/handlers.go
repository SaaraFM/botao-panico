// Package httpapi expõe o microserviço via API REST em JSON,
// usando apenas a biblioteca padrão (net/http).
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"

	"github.com/plataforma-mulher/botao-panico/internal/alerta"
)

// tamanhoMaximoCorpo limita o corpo das requisições (um alerta tem < 1 KB).
const tamanhoMaximoCorpo = 64 << 10 // 64 KiB

// ServicoAlertas é o que os handlers precisam — uma interface pequena,
// permitindo trocar a implementação real por fakes nos testes.
type ServicoAlertas interface {
	Acionar(ctx context.Context, pedido alerta.PedidoAlerta) (alerta.Alerta, error)
	Buscar(ctx context.Context, id string) (alerta.Alerta, error)
	Listar(ctx context.Context) ([]alerta.Alerta, error)
	Instituicoes(ctx context.Context) ([]alerta.InstituicaoApoio, error)
}

// Servidor reúne as dependências dos handlers HTTP.
type Servidor struct {
	servico ServicoAlertas
}

// NovoServidor cria o servidor HTTP com o serviço de alertas.
func NovoServidor(servico ServicoAlertas) *Servidor {
	return &Servidor{servico: servico}
}

// Rotas registra todos os endpoints do microserviço.
func (s *Servidor) Rotas() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.saude)
	mux.HandleFunc("POST /alertas", s.criarAlerta)
	mux.HandleFunc("GET /alertas", s.listarAlertas)
	mux.HandleFunc("GET /alertas/{id}", s.buscarAlerta)
	mux.HandleFunc("GET /instituicoes", s.listarInstituicoes)
	return mux
}

// saude é o health check do serviço.
func (s *Servidor) saude(w http.ResponseWriter, _ *http.Request) {
	responderJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// criarAlerta recebe o acionamento do botão de pânico (POST /alertas).
func (s *Servidor) criarAlerta(w http.ResponseWriter, r *http.Request) {
	var pedido alerta.PedidoAlerta
	if erroAPI := decodificarJSON(w, r, &pedido); erroAPI != nil {
		responderJSON(w, erroAPI.status, erroAPI.corpo)
		return
	}

	a, err := s.servico.Acionar(r.Context(), pedido)
	if err != nil {
		responderFalha(w, err)
		return
	}
	responderJSON(w, http.StatusCreated, a)
}

// listarAlertas devolve todos os alertas registrados (GET /alertas).
func (s *Servidor) listarAlertas(w http.ResponseWriter, r *http.Request) {
	lista, err := s.servico.Listar(r.Context())
	if err != nil {
		responderFalha(w, err)
		return
	}
	responderJSON(w, http.StatusOK, lista)
}

// buscarAlerta devolve um alerta específico com o resultado das notificações (GET /alertas/{id}).
func (s *Servidor) buscarAlerta(w http.ResponseWriter, r *http.Request) {
	a, err := s.servico.Buscar(r.Context(), r.PathValue("id"))
	if err != nil {
		responderFalha(w, err)
		return
	}
	responderJSON(w, http.StatusOK, a)
}

// listarInstituicoes devolve a rede de proteção cadastrada (GET /instituicoes).
func (s *Servidor) listarInstituicoes(w http.ResponseWriter, r *http.Request) {
	lista, err := s.servico.Instituicoes(r.Context())
	if err != nil {
		responderFalha(w, err)
		return
	}
	responderJSON(w, http.StatusOK, lista)
}

// decodificarJSON lê exatamente UM objeto JSON, com limite de tamanho e
// rejeitando campos desconhecidos.
func decodificarJSON(w http.ResponseWriter, r *http.Request, destino any) *erroHTTP {
	r.Body = http.MaxBytesReader(w, r.Body, tamanhoMaximoCorpo)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()

	if err := dec.Decode(destino); err != nil {
		var limite *http.MaxBytesError
		if errors.As(err, &limite) {
			return novoErro(http.StatusRequestEntityTooLarge, CodigoCorpoGrande, "corpo da requisição excede 64 KiB")
		}
		var tipo *json.UnmarshalTypeError
		if errors.As(err, &tipo) {
			return novoErro(http.StatusBadRequest, CodigoJSONInvalido, "tipo inválido no campo "+tipo.Field)
		}
		if errors.Is(err, io.EOF) {
			return novoErro(http.StatusBadRequest, CodigoJSONInvalido, "corpo da requisição vazio")
		}
		return novoErro(http.StatusBadRequest, CodigoJSONInvalido, "corpo da requisição não é um JSON válido")
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return novoErro(http.StatusBadRequest, CodigoJSONInvalido, "o corpo deve conter um único objeto JSON")
	}
	return nil
}

// responderJSON escreve qualquer valor como resposta JSON.
func responderJSON(w http.ResponseWriter, status int, valor any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(valor); err != nil {
		// O status já foi enviado; resta registrar (ex.: cliente desconectou).
		slog.Error("falha ao escrever resposta JSON", "erro", err)
	}
}
