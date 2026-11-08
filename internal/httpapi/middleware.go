package httpapi

import (
	"crypto/subtle"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/plataforma-mulher/botao-panico/internal/observabilidade"
)

// gravadorStatus armazena o status HTTP e a quantidade de bytes enviados para observabilidade.
type gravadorStatus struct {
	http.ResponseWriter
	status int
	bytes  int64
}

func (g *gravadorStatus) WriteHeader(status int) {
	g.status = status
	g.ResponseWriter.WriteHeader(status)
}

func (g *gravadorStatus) Write(b []byte) (int, error) {
	if g.status == 0 {
		g.status = http.StatusOK
	}
	n, err := g.ResponseWriter.Write(b)
	g.bytes += int64(n)
	return n, err
}

// LoggingMiddleware intercepta requisições HTTP e registra logs estruturados e métricas Prometheus.
// Propaga o Correlation ID via cabeçalhos e context.Context sem expor dados pessoais ou credenciais.
func LoggingMiddleware(proximo http.Handler, metricas ...*observabilidade.Metricas) http.Handler {
	var m *observabilidade.Metricas
	if len(metricas) > 0 {
		m = metricas[0]
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		inicio := time.Now()

		corrID := r.Header.Get("X-Correlation-ID")
		if corrID == "" {
			corrID = r.Header.Get("X-Request-ID")
		}
		corrID = observabilidade.ExtrairOuGerarCorrelationID(corrID)

		w.Header().Set("X-Request-ID", corrID)
		w.Header().Set("X-Correlation-ID", corrID)

		ctxComCorrelation := observabilidade.InjetarCorrelationID(r.Context(), corrID)
		r = r.WithContext(ctxComCorrelation)

		rec := &gravadorStatus{ResponseWriter: w, status: http.StatusOK}
		proximo.ServeHTTP(rec, r)

		duracao := time.Since(inicio)

		if m != nil {
			m.RegistrarRequisicaoHTTP(r.Method, r.URL.Path, rec.status, duracao.Seconds())
		}

		slog.Info("http_request",
			"correlation_id", corrID,
			"request_id", corrID,
			"metodo", r.Method,
			"rota", r.URL.Path,
			"status", rec.status,
			"duracao_ms", duracao.Milliseconds(),
			"bytes", rec.bytes,
		)
	})
}

// corsMiddleware aplica os cabeçalhos CORS necessários para o frontend React local.
func corsMiddleware(proximo http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Service-Token, X-Idempotency-Key, X-Request-ID, X-Correlation-ID")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		proximo.ServeHTTP(w, r)
	})
}

// isLocalhost verifica se a requisição é originada da máquina local (127.0.0.1, ::1 ou localhost).
func isLocalhost(remoteAddr string) bool {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		host = remoteAddr
	}
	return host == "127.0.0.1" || host == "::1" || host == "localhost"
}

// extrairToken obtém a credencial enviada via X-Service-Token ou Authorization: Bearer.
func extrairToken(r *http.Request) string {
	if tok := strings.TrimSpace(r.Header.Get("X-Service-Token")); tok != "" {
		return tok
	}
	auth := strings.TrimSpace(r.Header.Get("Authorization"))
	if len(auth) > 7 && strings.EqualFold(auth[:7], "bearer ") {
		return strings.TrimSpace(auth[7:])
	}
	return ""
}

// validarTokenConstante compara o token recebido com a credencial esperada em tempo constante,
// mitigando ataques de temporização (timing attacks).
func validarTokenConstante(fornecido, esperado string) bool {
	if len(esperado) == 0 {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(fornecido), []byte(esperado)) == 1
}

// autenticar protege endpoints sensíveis, rejeitando requisições sem credenciais válidas.
func (s *Servidor) autenticar(proximo http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Se a chave não foi configurada nesta instância (ex.: testes unitários sem auth), repassa
		if s.chaveServico == "" {
			proximo(w, r)
			return
		}

		token := extrairToken(r)
		if token == "" || !validarTokenConstante(token, s.chaveServico) {
			w.Header().Set("WWW-Authenticate", `Bearer realm="botao-panico", error="invalid_token"`)
			responderJSON(w, http.StatusUnauthorized, RespostaErro{
				Erro:   "autenticação obrigatória: credencial de serviço ausente ou inválida",
				Codigo: CodigoNaoAutorizado,
			})
			return
		}

		proximo(w, r)
	}
}
