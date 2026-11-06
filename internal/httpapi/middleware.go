package httpapi

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"log/slog"
	"net/http"
	"strings"
	"time"
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

// LoggingMiddleware intercepta requisições HTTP e registra métricas estruturadas de acesso
// (método, rota, código de status HTTP, tempo de execução e tamanho da resposta em bytes).
// Implementa o Requisito 8 de Observabilidade sem expor cabeçalhos Authorization ou dados de usuárias.
func LoggingMiddleware(proximo http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		inicio := time.Now()

		requestID := strings.TrimSpace(r.Header.Get("X-Request-ID"))
		if requestID == "" {
			b := make([]byte, 8)
			_, _ = rand.Read(b)
			requestID = "req-" + hex.EncodeToString(b)
		}
		w.Header().Set("X-Request-ID", requestID)

		rec := &gravadorStatus{ResponseWriter: w, status: http.StatusOK}
		proximo.ServeHTTP(rec, r)

		duracaoMs := time.Since(inicio).Milliseconds()

		slog.Info("http_request",
			"request_id", requestID,
			"metodo", r.Method,
			"rota", r.URL.Path,
			"status", rec.status,
			"duracao_ms", duracaoMs,
			"bytes", rec.bytes,
		)
	})
}

// corsMiddleware aplica os cabeçalhos CORS necessários para o frontend React local.
func corsMiddleware(proximo http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Service-Token, X-Idempotency-Key, X-Request-ID")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		proximo.ServeHTTP(w, r)
	})
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
