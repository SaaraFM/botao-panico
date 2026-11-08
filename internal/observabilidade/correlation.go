package observabilidade

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"regexp"
	"strings"
)

type chaveContexto string

const chaveCorrelationID chaveContexto = "correlation_id"

// regexCorrelationID valida identificadores seguros (alfanumérico, hífen, underline e ponto).
// Evita injeção de cabeçalhos, quebras de linha ou caracteres perigosos em logs estruturados.
var regexCorrelationID = regexp.MustCompile(`^[a-zA-Z0-9_\-\.]{1,128}$`)

// ExtrairOuGerarCorrelationID valida e sanitiza o identificador recebido externamente.
// Se estiver vazio, for inválido ou exceder 128 caracteres, gera um novo identificador seguro.
func ExtrairOuGerarCorrelationID(raw string) string {
	limpo := strings.TrimSpace(raw)
	if limpo != "" && len(limpo) <= 128 && regexCorrelationID.MatchString(limpo) {
		return limpo
	}
	return GerarCorrelationID()
}

// GerarCorrelationID cria um identificador criptograficamente seguro no formato trace-<16-hex>.
func GerarCorrelationID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return "trace-" + hex.EncodeToString(b)
}

// InjetarCorrelationID associa o correlation ID ao context.Context.
func InjetarCorrelationID(ctx context.Context, id string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	idValido := ExtrairOuGerarCorrelationID(id)
	return context.WithValue(ctx, chaveCorrelationID, idValido)
}

// ObterCorrelationID extrai o correlation ID do context.Context ou gera um novo caso ausente.
func ObterCorrelationID(ctx context.Context) string {
	if ctx != nil {
		if val, ok := ctx.Value(chaveCorrelationID).(string); ok && val != "" {
			return val
		}
	}
	return GerarCorrelationID()
}
