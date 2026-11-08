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
	"strings"
	"time"

	"github.com/plataforma-mulher/botao-panico/internal/alerta"
	"github.com/plataforma-mulher/botao-panico/internal/observabilidade"
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

// OpcoesServidor define parâmetros opcionais e injeção de dependências de observabilidade.
type OpcoesServidor struct {
	ChaveServico                  string
	Metricas                      *observabilidade.Metricas
	MetricasAtivas                bool
	MetricasAcessoLocalIrrestrito bool
	VerificadorSaude              func(context.Context) map[string]any
}

// Servidor reúne as dependências dos handlers HTTP.
type Servidor struct {
	servico                       ServicoAlertas
	chaveServico                  string
	metricas                      *observabilidade.Metricas
	metricasAtivas                bool
	metricasAcessoLocalIrrestrito bool
	verificadorSaude              func(context.Context) map[string]any
}

// NovoServidor cria o servidor HTTP com o serviço de alertas e chave de autenticação opcional.
func NovoServidor(servico ServicoAlertas, chaveServico ...string) *Servidor {
	var chave string
	if len(chaveServico) > 0 {
		chave = strings.TrimSpace(chaveServico[0])
	}
	return NovoServidorComOpcoes(servico, OpcoesServidor{
		ChaveServico:                  chave,
		MetricasAtivas:                true,
		MetricasAcessoLocalIrrestrito: true,
	})
}

// NovoServidorComOpcoes cria o servidor HTTP com opções completas de observabilidade e segurança.
func NovoServidorComOpcoes(servico ServicoAlertas, opts OpcoesServidor) *Servidor {
	m := opts.Metricas
	if opts.MetricasAtivas && m == nil {
		m = observabilidade.NovaMetricas()
	}
	return &Servidor{
		servico:                       servico,
		chaveServico:                  strings.TrimSpace(opts.ChaveServico),
		metricas:                      m,
		metricasAtivas:                opts.MetricasAtivas,
		metricasAcessoLocalIrrestrito: opts.MetricasAcessoLocalIrrestrito,
		verificadorSaude:              opts.VerificadorSaude,
	}
}

// Rotas registra todos os endpoints do microserviço com observabilidade estruturada e CORS.
func (s *Servidor) Rotas() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.saude)
	mux.HandleFunc("GET /healthz", s.saude)
	mux.HandleFunc("GET /status", s.statusDetalhado)
	mux.HandleFunc("GET /metrics", s.metricasHandler)
	mux.HandleFunc("POST /alertas", s.autenticar(s.criarAlerta))
	mux.HandleFunc("GET /alertas", s.autenticar(s.listarAlertas))
	mux.HandleFunc("GET /alertas/{id}", s.autenticar(s.buscarAlerta))
	mux.HandleFunc("GET /instituicoes", s.listarInstituicoes)
	return LoggingMiddleware(corsMiddleware(mux), s.metricas)
}

// saude é o health check simples (liveness) do serviço.
func (s *Servidor) saude(w http.ResponseWriter, _ *http.Request) {
	responderJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// statusDetalhado é o health check aprofundado (readiness) com status dos componentes.
func (s *Servidor) statusDetalhado(w http.ResponseWriter, r *http.Request) {
	statusGeral := "ok"
	comps := make(map[string]any)
	if s.verificadorSaude != nil {
		comps = s.verificadorSaude(r.Context())
		for _, v := range comps {
			if vMap, ok := v.(map[string]any); ok {
				if st, ok := vMap["status"].(string); ok && st != "ok" {
					statusGeral = "degradado"
				}
			}
		}
	}

	resposta := map[string]any{
		"status":      statusGeral,
		"servico":     "botao-panico",
		"timestamp":   time.Now().UTC().Format(time.RFC3339),
		"componentes": comps,
	}
	responderJSON(w, http.StatusOK, resposta)
}

// metricasHandler expõe as métricas Prometheus garantindo proteção de acesso.
func (s *Servidor) metricasHandler(w http.ResponseWriter, r *http.Request) {
	if !s.metricasAtivas {
		responderJSON(w, http.StatusNotFound, RespostaErro{
			Erro:   "endpoint de métricas desativado",
			Codigo: CodigoNaoEncontrado,
		})
		return
	}

	// Permite acesso irrestrito a localhost (desenvolvimento/testes/scraping local) se configurado
	permitidoLocal := s.metricasAcessoLocalIrrestrito && isLocalhost(r.RemoteAddr)
	if !permitidoLocal && s.chaveServico != "" {
		token := extrairToken(r)
		if token == "" || !validarTokenConstante(token, s.chaveServico) {
			w.Header().Set("WWW-Authenticate", `Bearer realm="botao-panico", error="invalid_token"`)
			responderJSON(w, http.StatusUnauthorized, RespostaErro{
				Erro:   "autenticação obrigatória para acesso externo às métricas",
				Codigo: CodigoNaoAutorizado,
			})
			return
		}
	}

	if s.metricas != nil {
		s.metricas.Handler().ServeHTTP(w, r)
		return
	}
	responderJSON(w, http.StatusOK, map[string]string{"status": "metricas_vazias"})
}

// criarAlerta recebe o acionamento do botão de pânico (POST /alertas).
func (s *Servidor) criarAlerta(w http.ResponseWriter, r *http.Request) {
	var pedido alerta.PedidoAlerta
	if erroAPI := decodificarJSON(w, r, &pedido); erroAPI != nil {
		responderJSON(w, erroAPI.status, erroAPI.corpo)
		return
	}

	// Permite enviar a chave de idempotência tanto via cabeçalho HTTP quanto no payload
	if chaveHeader := strings.TrimSpace(r.Header.Get("X-Idempotency-Key")); chaveHeader != "" && pedido.ChaveIdempotencia == "" {
		pedido.ChaveIdempotencia = chaveHeader
	}

	a, err := s.servico.Acionar(r.Context(), pedido)
	if err != nil {
		responderFalha(w, err)
		return
	}

	if a.Replay {
		w.Header().Set("X-Idempotent-Replay", "true")
		responderJSON(w, http.StatusOK, a)
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
