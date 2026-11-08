package grpcapi

import (
	"context"
	"crypto/subtle"
	"log/slog"
	"strings"
	"time"

	"github.com/plataforma-mulher/botao-panico/internal/observabilidade"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// extrairTokenGRPC busca o token de serviço nos metadados gRPC.
// Aceita tanto 'x-service-token' quanto 'authorization: Bearer <token>'.
func extrairTokenGRPC(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}

	// 1. Tenta cabeçalho direto de serviço
	if tokens := md.Get("x-service-token"); len(tokens) > 0 && strings.TrimSpace(tokens[0]) != "" {
		return strings.TrimSpace(tokens[0])
	}

	// 2. Tenta Authorization: Bearer
	if auths := md.Get("authorization"); len(auths) > 0 {
		auth := strings.TrimSpace(auths[0])
		if len(auth) > 7 && strings.EqualFold(auth[:7], "bearer ") {
			return strings.TrimSpace(auth[7:])
		}
	}

	return ""
}

// extrairCorrelationIDGRPC busca o identificador nos metadados gRPC (x-correlation-id ou x-request-id).
func extrairCorrelationIDGRPC(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if ok {
		if ids := md.Get("x-correlation-id"); len(ids) > 0 && strings.TrimSpace(ids[0]) != "" {
			return observabilidade.ExtrairOuGerarCorrelationID(ids[0])
		}
		if ids := md.Get("x-request-id"); len(ids) > 0 && strings.TrimSpace(ids[0]) != "" {
			return observabilidade.ExtrairOuGerarCorrelationID(ids[0])
		}
	}
	return observabilidade.GerarCorrelationID()
}

// validarCredencial compara o token recebido com a chave esperada em tempo constante.
func validarCredencial(token, chaveEsperada string) bool {
	if len(chaveEsperada) == 0 {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(token), []byte(chaveEsperada)) == 1
}

// AutenticarUnary intercepta chamadas unárias (requisição única -> resposta única) e valida a credencial S2S.
func AutenticarUnary(chaveServico string) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		if chaveServico == "" {
			return handler(ctx, req)
		}

		token := extrairTokenGRPC(ctx)
		if token == "" || !validarCredencial(token, chaveServico) {
			return nil, status.Error(
				codes.Unauthenticated,
				"autenticação gRPC obrigatória: credencial de serviço ausente ou inválida",
			)
		}

		return handler(ctx, req)
	}
}

// AutenticarStream intercepta chamadas gRPC por streaming e valida a credencial antes de abrir o stream.
func AutenticarStream(chaveServico string) grpc.StreamServerInterceptor {
	return func(
		srv any,
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		if chaveServico == "" {
			return handler(srv, ss)
		}

		token := extrairTokenGRPC(ss.Context())
		if token == "" || !validarCredencial(token, chaveServico) {
			return status.Error(
				codes.Unauthenticated,
				"autenticação gRPC obrigatória: credencial de serviço ausente ou inválida no stream",
			)
		}

		return handler(srv, ss)
	}
}

// LoggingUnary intercepta chamadas gRPC e emite logs estruturados e métricas Prometheus com método, duração e status.
func LoggingUnary(metricas ...*observabilidade.Metricas) grpc.UnaryServerInterceptor {
	var m *observabilidade.Metricas
	if len(metricas) > 0 {
		m = metricas[0]
	}

	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		inicio := time.Now()
		corrID := extrairCorrelationIDGRPC(ctx)
		ctxComCorr := observabilidade.InjetarCorrelationID(ctx, corrID)

		resp, err := handler(ctxComCorr, req)
		duracao := time.Since(inicio)
		st, _ := status.FromError(err)

		if m != nil {
			m.RegistrarChamadaGRPC(info.FullMethod, st.Code().String(), duracao.Seconds())
		}

		slog.Info("grpc_request",
			"correlation_id", corrID,
			"metodo", info.FullMethod,
			"codigo", st.Code().String(),
			"duracao_ms", duracao.Milliseconds(),
			"sucesso", err == nil,
		)
		return resp, err
	}
}
