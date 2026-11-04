package httpapi

import (
	"crypto/subtle"
	"net/http"
	"strings"
)

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
