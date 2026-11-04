package grpcapi

import (
	"context"
	"errors"
	"strings"

	"github.com/plataforma-mulher/botao-panico/gen/alerta/v1"
	"github.com/plataforma-mulher/botao-panico/internal/alerta"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// deProtoNivel traduz o enum Protocol Buffers para o tipo de domínio Go.
func deProtoNivel(n alertav1.NivelRisco) alerta.NivelRisco {
	switch n {
	case alertav1.NivelRisco_BAIXO:
		return alerta.RiscoBaixo
	case alertav1.NivelRisco_MODERADO:
		return alerta.RiscoModerado
	case alertav1.NivelRisco_ALTO:
		return alerta.RiscoAlto
	case alertav1.NivelRisco_CRITICO:
		return alerta.RiscoCritico
	default:
		return alerta.NivelRisco(strings.ToLower(n.String()))
	}
}

// paraProtoNivel traduz o tipo de domínio Go para o enum Protocol Buffers.
func paraProtoNivel(n alerta.NivelRisco) alertav1.NivelRisco {
	switch n {
	case alerta.RiscoBaixo:
		return alertav1.NivelRisco_BAIXO
	case alerta.RiscoModerado:
		return alertav1.NivelRisco_MODERADO
	case alerta.RiscoAlto:
		return alertav1.NivelRisco_ALTO
	case alerta.RiscoCritico:
		return alertav1.NivelRisco_CRITICO
	default:
		return alertav1.NivelRisco_NIVEL_RISCO_UNSPECIFIED
	}
}

// paraProtoStatus traduz o status de domínio para Protocol Buffers.
func paraProtoStatus(s alerta.StatusAlerta) alertav1.StatusAlerta {
	switch s {
	case alerta.StatusRecebido:
		return alertav1.StatusAlerta_CRIADO
	case alerta.StatusNotificando:
		return alertav1.StatusAlerta_PROCESSANDO
	case alerta.StatusConcluido:
		return alertav1.StatusAlerta_CONCLUIDO
	default:
		return alertav1.StatusAlerta_STATUS_UNSPECIFIED
	}
}

// paraProtoAlerta converte o registro de domínio Alerta no formato protobuf.
func paraProtoAlerta(a alerta.Alerta) *alertav1.Alerta {
	var criadoEm *timestamppb.Timestamp
	if !a.CriadoEm.IsZero() {
		criadoEm = timestamppb.New(a.CriadoEm)
	}

	notificacoes := make([]*alertav1.Notificacao, 0, len(a.Notificacoes))
	for _, n := range a.Notificacoes {
		notificacoes = append(notificacoes, &alertav1.Notificacao{
			Canal:        n.Canal,
			Destinatario: n.Destino,
			Sucesso:      n.Sucesso,
			Erro:         n.Erro,
		})
	}

	return &alertav1.Alerta{
		Id:        a.ID,
		UsuariaId: a.UsuariaID,
		Nivel:     paraProtoNivel(a.Nivel),
		Mensagem:  a.Mensagem,
		Localizacao: &alertav1.Coordenadas{
			Latitude:  a.Local.Latitude,
			Longitude: a.Local.Longitude,
		},
		EnderecoAproximado: a.EnderecoAproximado,
		Status:             paraProtoStatus(a.Status),
		CriadoEm:           criadoEm,
		Notificacoes:       notificacoes,
		Replay:             a.Replay,
	}
}

// paraProtoInstituicao converte uma instituição de apoio de domínio em protobuf.
func paraProtoInstituicao(i alerta.InstituicaoApoio) *alertav1.InstituicaoApoio {
	return &alertav1.InstituicaoApoio{
		Nome:     i.Nome,
		Telefone: i.Telefone,
		Tipo:     i.Tipo,
	}
}

// converterErro traduz erros da camada de domínio em códigos de status gRPC sem vazar dados sensíveis.
func converterErro(err error) error {
	if err == nil {
		return nil
	}

	// 1. Respeita cancelamento ou timeout do contexto
	if errors.Is(err, context.Canceled) {
		return status.Error(codes.Canceled, "operação cancelada pelo cliente")
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return status.Error(codes.DeadlineExceeded, "prazo limite (deadline) da operação esgotado")
	}

	// 2. Erros de validação (regras de negócio)
	var errVal *alerta.ErrosValidacao
	if errors.As(err, &errVal) {
		return status.Errorf(codes.InvalidArgument, "parâmetros inválidos: %s", errVal.Error())
	}

	// 3. Conflito de idempotência
	if errors.Is(err, alerta.ErrConflitoIdempotencia) {
		return status.Error(codes.AlreadyExists, "chave de idempotência já utilizada com dados diferentes")
	}

	// 4. Registro não encontrado
	if errors.Is(err, alerta.ErrNaoEncontrado) {
		return status.Error(codes.NotFound, "alerta não encontrado")
	}

	// 5. Falhas de infraestrutura de banco
	if errors.Is(err, alerta.ErrBancoIndisponivel) {
		return status.Error(codes.Unavailable, "serviço temporariamente indisponível: falha de banco de dados")
	}

	if errors.Is(err, alerta.ErrEscrita) || errors.Is(err, alerta.ErrLeitura) {
		return status.Error(codes.Internal, "falha interna ao processar persistência de dados")
	}

	return status.Errorf(codes.Internal, "erro interno: %v", err)
}
