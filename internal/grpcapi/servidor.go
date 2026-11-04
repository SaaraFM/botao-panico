package grpcapi

import (
	"context"
	"strings"

	"github.com/plataforma-mulher/botao-panico/gen/alerta/v1"
	"github.com/plataforma-mulher/botao-panico/internal/alerta"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ServicoAlertas define a interface do domínio necessária para os handlers gRPC.
type ServicoAlertas interface {
	Acionar(ctx context.Context, pedido alerta.PedidoAlerta) (alerta.Alerta, error)
	Buscar(ctx context.Context, id string) (alerta.Alerta, error)
	Listar(ctx context.Context) ([]alerta.Alerta, error)
	Instituicoes(ctx context.Context) ([]alerta.InstituicaoApoio, error)
}

// Servidor implementa o serviço gRPC AlertasServiceServer gerado pelo Protocol Buffers.
type Servidor struct {
	alertav1.UnimplementedAlertasServiceServer
	servico      ServicoAlertas
	chaveServico string
}

// NovoServidor inicializa a implementação gRPC com suas dependências de domínio e chave S2S.
func NovoServidor(servico ServicoAlertas, chaveServico ...string) *Servidor {
	var chave string
	if len(chaveServico) > 0 {
		chave = strings.TrimSpace(chaveServico[0])
	}
	return &Servidor{
		servico:      servico,
		chaveServico: chave,
	}
}

// RegistrarServico conecta o servidor gRPC ao handler protobuf registrado.
func (s *Servidor) RegistrarServico(gServer *grpc.Server) {
	alertav1.RegisterAlertasServiceServer(gServer, s)
}

// CriarAlerta processa o acionamento de um alerta de pânico via gRPC.
func (s *Servidor) CriarAlerta(
	ctx context.Context,
	req *alertav1.CriarAlertaRequest,
) (*alertav1.CriarAlertaResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "requisição não pode ser nula")
	}

	var local alerta.Localizacao
	if req.Localizacao != nil {
		local = alerta.Localizacao{
			Latitude:  req.Localizacao.Latitude,
			Longitude: req.Localizacao.Longitude,
		}
	}

	pedido := alerta.PedidoAlerta{
		ChaveIdempotencia: req.ChaveIdempotencia,
		UsuariaID:         req.UsuariaId,
		Nivel:             deProtoNivel(req.Nivel),
		Mensagem:          req.Mensagem,
		Local:             local,
		Silencioso:        req.Silencioso,
	}

	resultado, err := s.servico.Acionar(ctx, pedido)
	if err != nil {
		return nil, converterErro(err)
	}

	return &alertav1.CriarAlertaResponse{
		Alerta: paraProtoAlerta(resultado),
		Replay: resultado.Replay,
	}, nil
}

// BuscarAlerta consulta um alerta existente pelo seu identificador único.
func (s *Servidor) BuscarAlerta(
	ctx context.Context,
	req *alertav1.BuscarAlertaRequest,
) (*alertav1.BuscarAlertaResponse, error) {
	if req == nil || strings.TrimSpace(req.Id) == "" {
		return nil, status.Error(codes.InvalidArgument, "id do alerta é obrigatório")
	}

	resultado, err := s.servico.Buscar(ctx, strings.TrimSpace(req.Id))
	if err != nil {
		return nil, converterErro(err)
	}

	return &alertav1.BuscarAlertaResponse{
		Alerta: paraProtoAlerta(resultado),
	}, nil
}

// ListarAlertas retorna os alertas da usuária solicitante.
// Regra de segurança CIT7219: exige usuaria_id para evitar listagem global indiscriminada de dados pessoais.
func (s *Servidor) ListarAlertas(
	ctx context.Context,
	req *alertav1.ListarAlertasRequest,
) (*alertav1.ListarAlertasResponse, error) {
	if req == nil || strings.TrimSpace(req.UsuariaId) == "" {
		return nil, status.Error(
			codes.InvalidArgument,
			"usuaria_id é obrigatório para consultar o histórico; listagem global indiscriminada é restrita por segurança",
		)
	}

	todos, err := s.servico.Listar(ctx)
	if err != nil {
		return nil, converterErro(err)
	}

	usuariaAlvo := strings.TrimSpace(req.UsuariaId)
	filtrados := make([]*alertav1.Alerta, 0)
	for _, a := range todos {
		if a.UsuariaID == usuariaAlvo {
			filtrados = append(filtrados, paraProtoAlerta(a))
		}
	}

	return &alertav1.ListarAlertasResponse{
		Alertas: filtrados,
	}, nil
}

// ListarInstituicoes retorna as instituições da rede de apoio cadastradas.
func (s *Servidor) ListarInstituicoes(
	ctx context.Context,
	_ *alertav1.ListarInstituicoesRequest,
) (*alertav1.ListarInstituicoesResponse, error) {
	instituicoes, err := s.servico.Instituicoes(ctx)
	if err != nil {
		return nil, converterErro(err)
	}

	protoInstituicoes := make([]*alertav1.InstituicaoApoio, 0, len(instituicoes))
	for _, inst := range instituicoes {
		protoInstituicoes = append(protoInstituicoes, paraProtoInstituicao(inst))
	}

	return &alertav1.ListarInstituicoesResponse{
		Instituicoes: protoInstituicoes,
	}, nil
}
