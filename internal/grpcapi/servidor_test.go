package grpcapi_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/plataforma-mulher/botao-panico/gen/alerta/v1"
	"github.com/plataforma-mulher/botao-panico/internal/alerta"
	"github.com/plataforma-mulher/botao-panico/internal/armazenamento"
	"github.com/plataforma-mulher/botao-panico/internal/grpcapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

const (
	tamanhoBufferTest = 1024 * 1024
	chaveTesteValida  = "chave-secreta-grpc-12345"
)

type fakeDespachante struct{}

func (f fakeDespachante) Despachar(_ context.Context, dests []alerta.Destinatario, _ string) []alerta.ResultadoEnvio {
	res := make([]alerta.ResultadoEnvio, len(dests))
	for i, d := range dests {
		res[i] = alerta.ResultadoEnvio{
			Destino:   d.Nome,
			Canal:     "sms",
			Sucesso:   true,
			DuracaoMs: 5,
			EnviadoEm: time.Now().Format(time.RFC3339),
		}
	}
	return res
}

func criarAmbienteTesteGRPC(t *testing.T, chaveServico string) (alertav1.AlertasServiceClient, *alerta.Servico, func()) {
	t.Helper()

	repo := armazenamento.NovaMemoria()
	desp := fakeDespachante{}
	servico := alerta.NovoServico(repo, desp, nil, 2*time.Second)

	listener := bufconn.Listen(tamanhoBufferTest)

	var opts []grpc.ServerOption
	if chaveServico != "" {
		opts = append(opts,
			grpc.UnaryInterceptor(grpcapi.AutenticarUnary(chaveServico)),
			grpc.StreamInterceptor(grpcapi.AutenticarStream(chaveServico)),
		)
	}

	gServer := grpc.NewServer(opts...)
	srv := grpcapi.NovoServidor(servico, chaveServico)
	srv.RegistrarServico(gServer)

	go func() {
		_ = gServer.Serve(listener)
	}()

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)

	client := alertav1.NewAlertasServiceClient(conn)

	limpeza := func() {
		_ = conn.Close()
		gServer.GracefulStop()
		_ = listener.Close()
	}

	return client, servico, limpeza
}

func contextComToken(token string) context.Context {
	md := metadata.Pairs("x-service-token", token)
	return metadata.NewOutgoingContext(context.Background(), md)
}

func contextComBearer(token string) context.Context {
	md := metadata.Pairs("authorization", "Bearer "+token)
	return metadata.NewOutgoingContext(context.Background(), md)
}

// 1. CriarAlerta com sucesso
func TestGRPC_CriarAlertaSucesso(t *testing.T) {
	client, _, cleanup := criarAmbienteTesteGRPC(t, "")
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	req := &alertav1.CriarAlertaRequest{
		UsuariaId: "usuaria-teste-grpc-1",
		Nivel:     alertav1.NivelRisco_ALTO,
		Mensagem:  "Socorro gRPC teste",
		Localizacao: &alertav1.Coordenadas{
			Latitude:  -28.9346,
			Longitude: -49.4957,
		},
		ChaveIdempotencia: "idemp-grpc-001",
	}

	resp, err := client.CriarAlerta(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.NotEmpty(t, resp.Alerta.Id)
	assert.Equal(t, "usuaria-teste-grpc-1", resp.Alerta.UsuariaId)
	assert.Equal(t, alertav1.NivelRisco_ALTO, resp.Alerta.Nivel)
	assert.Equal(t, alertav1.StatusAlerta_CONCLUIDO, resp.Alerta.Status)
	assert.False(t, resp.Replay)
	assert.NotEmpty(t, resp.Alerta.Notificacoes)
}

// 2. Autenticação gRPC (x-service-token e Bearer)
func TestGRPC_AutenticacaoS2S(t *testing.T) {
	client, _, cleanup := criarAmbienteTesteGRPC(t, chaveTesteValida)
	defer cleanup()

	req := &alertav1.CriarAlertaRequest{
		UsuariaId: "usuaria-auth-grpc",
		Nivel:     alertav1.NivelRisco_MODERADO,
		Localizacao: &alertav1.Coordenadas{
			Latitude:  -28.93,
			Longitude: -49.49,
		},
	}

	// 2.1 Sem metadados de autenticação -> 401 Unauthenticated
	_, err := client.CriarAlerta(context.Background(), req)
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.Unauthenticated, st.Code())

	// 2.2 Com token incorreto -> 401 Unauthenticated
	ctxInvalido := contextComToken("token-totalmente-errado")
	_, err = client.CriarAlerta(ctxInvalido, req)
	require.Error(t, err)
	st, _ = status.FromError(err)
	assert.Equal(t, codes.Unauthenticated, st.Code())

	// 2.3 Com x-service-token correto -> Sucesso
	ctxValidoHeader := contextComToken(chaveTesteValida)
	resp, err := client.CriarAlerta(ctxValidoHeader, req)
	require.NoError(t, err)
	assert.NotEmpty(t, resp.Alerta.Id)

	// 2.4 Com Authorization: Bearer correto -> Sucesso
	ctxValidoBearer := contextComBearer(chaveTesteValida)
	req2 := &alertav1.CriarAlertaRequest{
		UsuariaId: "usuaria-auth-bearer",
		Nivel:     alertav1.NivelRisco_BAIXO,
		Localizacao: &alertav1.Coordenadas{
			Latitude:  -28.93,
			Longitude: -49.49,
		},
	}
	resp2, err := client.CriarAlerta(ctxValidoBearer, req2)
	require.NoError(t, err)
	assert.NotEmpty(t, resp2.Alerta.Id)
}

// 3. Validação de parâmetros (rejeição com InvalidArgument)
func TestGRPC_ValidacaoEntrada(t *testing.T) {
	client, _, cleanup := criarAmbienteTesteGRPC(t, "")
	defer cleanup()

	// 3.1 Usuária vazia
	reqSemUsuaria := &alertav1.CriarAlertaRequest{
		UsuariaId: "",
		Nivel:     alertav1.NivelRisco_ALTO,
		Localizacao: &alertav1.Coordenadas{
			Latitude:  -28.93,
			Longitude: -49.49,
		},
	}
	_, err := client.CriarAlerta(context.Background(), reqSemUsuaria)
	require.Error(t, err)
	st, _ := status.FromError(err)
	assert.Equal(t, codes.InvalidArgument, st.Code())

	// 3.2 Latitude fora da faixa
	reqLatInvalida := &alertav1.CriarAlertaRequest{
		UsuariaId: "u1",
		Nivel:     alertav1.NivelRisco_ALTO,
		Localizacao: &alertav1.Coordenadas{
			Latitude:  95.0, // Inválido > 90
			Longitude: -49.49,
		},
	}
	_, err = client.CriarAlerta(context.Background(), reqLatInvalida)
	require.Error(t, err)
	st, _ = status.FromError(err)
	assert.Equal(t, codes.InvalidArgument, st.Code())
}

// 4. Idempotência via gRPC
func TestGRPC_Idempotencia(t *testing.T) {
	client, servico, cleanup := criarAmbienteTesteGRPC(t, "")
	defer cleanup()

	chaveIdemp := "chave-compartilhada-123"
	reqOriginal := &alertav1.CriarAlertaRequest{
		ChaveIdempotencia: chaveIdemp,
		UsuariaId:         "usuaria-idemp-1",
		Nivel:             alertav1.NivelRisco_CRITICO,
		Mensagem:          "Perigo iminente",
		Localizacao: &alertav1.Coordenadas{
			Latitude:  -28.9346,
			Longitude: -49.4957,
		},
	}

	// 4.1 Primeira tentativa cria
	resp1, err := client.CriarAlerta(context.Background(), reqOriginal)
	require.NoError(t, err)
	assert.False(t, resp1.Replay)
	idOriginal := resp1.Alerta.Id

	// 4.2 Retentativa idêntica retorna o mesmo alerta com replay = true
	resp2, err := client.CriarAlerta(context.Background(), reqOriginal)
	require.NoError(t, err)
	assert.True(t, resp2.Replay)
	assert.Equal(t, idOriginal, resp2.Alerta.Id)

	// 4.3 Tentativa com mesma chave e conteúdo divergente retorna AlreadyExists (HTTP 409 equivalente)
	reqDivergente := &alertav1.CriarAlertaRequest{
		ChaveIdempotencia: chaveIdemp,
		UsuariaId:         "usuaria-idemp-1",
		Nivel:             alertav1.NivelRisco_BAIXO, // Mudou o nível
		Mensagem:          "Perigo iminente",
		Localizacao: &alertav1.Coordenadas{
			Latitude:  -28.9346,
			Longitude: -49.4957,
		},
	}
	_, err = client.CriarAlerta(context.Background(), reqDivergente)
	require.Error(t, err)
	st, _ := status.FromError(err)
	assert.Equal(t, codes.AlreadyExists, st.Code())

	// 4.4 Idempotência compartilhada: o alerta criado via gRPC pode ser recuperado diretamente pelo domínio (compatível com REST)
	alertaRecuperado, err := servico.Buscar(context.Background(), idOriginal)
	require.NoError(t, err)
	assert.Equal(t, chaveIdemp, alertaRecuperado.ChaveIdempotencia)
}

// 5. BuscarAlerta por ID
func TestGRPC_BuscarAlerta(t *testing.T) {
	client, _, cleanup := criarAmbienteTesteGRPC(t, "")
	defer cleanup()

	// 5.1 Cria para ter o que buscar
	respCria, err := client.CriarAlerta(context.Background(), &alertav1.CriarAlertaRequest{
		UsuariaId: "usuaria-busca",
		Nivel:     alertav1.NivelRisco_MODERADO,
		Localizacao: &alertav1.Coordenadas{
			Latitude:  -28.9,
			Longitude: -49.4,
		},
	})
	require.NoError(t, err)

	// 5.2 Busca existente
	respBusca, err := client.BuscarAlerta(context.Background(), &alertav1.BuscarAlertaRequest{
		Id: respCria.Alerta.Id,
	})
	require.NoError(t, err)
	assert.Equal(t, respCria.Alerta.Id, respBusca.Alerta.Id)
	assert.Equal(t, "usuaria-busca", respBusca.Alerta.UsuariaId)

	// 5.3 Busca inexistente -> NotFound
	_, err = client.BuscarAlerta(context.Background(), &alertav1.BuscarAlertaRequest{
		Id: "id-totalmente-inexistente",
	})
	require.Error(t, err)
	st, _ := status.FromError(err)
	assert.Equal(t, codes.NotFound, st.Code())

	// 5.4 ID vazio -> InvalidArgument
	_, err = client.BuscarAlerta(context.Background(), &alertav1.BuscarAlertaRequest{
		Id: "",
	})
	require.Error(t, err)
	st, _ = status.FromError(err)
	assert.Equal(t, codes.InvalidArgument, st.Code())
}

// 6. ListarAlertas com restrição de privacidade (usuaria_id obrigatório)
func TestGRPC_ListarAlertasSeguranca(t *testing.T) {
	client, _, cleanup := criarAmbienteTesteGRPC(t, "")
	defer cleanup()

	// 6.1 Cria alertas de duas usuárias distintas
	_, err := client.CriarAlerta(context.Background(), &alertav1.CriarAlertaRequest{
		UsuariaId: "maria",
		Nivel:     alertav1.NivelRisco_ALTO,
		Localizacao: &alertav1.Coordenadas{
			Latitude:  -28.9,
			Longitude: -49.4,
		},
	})
	require.NoError(t, err)

	_, err = client.CriarAlerta(context.Background(), &alertav1.CriarAlertaRequest{
		UsuariaId: "joana",
		Nivel:     alertav1.NivelRisco_BAIXO,
		Localizacao: &alertav1.Coordenadas{
			Latitude:  -28.9,
			Longitude: -49.4,
		},
	})
	require.NoError(t, err)

	// 6.2 Tentativa de listagem global indiscriminada (sem usuaria_id) DEVE ser rejeitada por segurança
	_, err = client.ListarAlertas(context.Background(), &alertav1.ListarAlertasRequest{
		UsuariaId: "",
	})
	require.Error(t, err)
	st, _ := status.FromError(err)
	assert.Equal(t, codes.InvalidArgument, st.Code())
	assert.Contains(t, st.Message(), "usuaria_id é obrigatório")

	// 6.3 Listagem por usuária retorna estritamente os alertas daquela usuária
	respMaria, err := client.ListarAlertas(context.Background(), &alertav1.ListarAlertasRequest{
		UsuariaId: "maria",
	})
	require.NoError(t, err)
	assert.Len(t, respMaria.Alertas, 1)
	assert.Equal(t, "maria", respMaria.Alertas[0].UsuariaId)
}

// 7. ListarInstituicoes de proteção
func TestGRPC_ListarInstituicoes(t *testing.T) {
	client, _, cleanup := criarAmbienteTesteGRPC(t, "")
	defer cleanup()

	resp, err := client.ListarInstituicoes(context.Background(), &alertav1.ListarInstituicoesRequest{})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.NotEmpty(t, resp.Instituicoes)
	assert.Contains(t, resp.Instituicoes[0].Nome, "Polícia Militar")
}

// 8. Respeito a Deadline e Cancelamento de Contexto
func TestGRPC_ContextoECancelamento(t *testing.T) {
	client, _, cleanup := criarAmbienteTesteGRPC(t, "")
	defer cleanup()

	// 8.1 Contexto pré-cancelado
	ctxCancelado, cancel := context.WithCancel(context.Background())
	cancel() // cancela antes de chamar

	req := &alertav1.CriarAlertaRequest{
		UsuariaId: "usuaria-cancel",
		Nivel:     alertav1.NivelRisco_ALTO,
		Localizacao: &alertav1.Coordenadas{
			Latitude:  -28.9,
			Longitude: -49.4,
		},
	}

	_, err := client.CriarAlerta(ctxCancelado, req)
	require.Error(t, err)
	st, _ := status.FromError(err)
	assert.Equal(t, codes.Canceled, st.Code())

	// 8.2 Contexto com deadline expirada
	ctxExpirado, cancelTimeout := context.WithDeadline(context.Background(), time.Now().Add(-1*time.Second))
	defer cancelTimeout()

	_, err = client.CriarAlerta(ctxExpirado, req)
	require.Error(t, err)
	st, _ = status.FromError(err)
	assert.Equal(t, codes.DeadlineExceeded, st.Code())
}

// mockServerStream simula um stream gRPC para validação do interceptor de streaming
type mockServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (m *mockServerStream) Context() context.Context {
	return m.ctx
}

// 9. Interceptor de Streaming para evolução futura
func TestGRPC_StreamInterceptor(t *testing.T) {
	interceptor := grpcapi.AutenticarStream(chaveTesteValida)

	info := &grpc.StreamServerInfo{
		FullMethod:     "/alerta.v1.AlertasService/MonitorarAlertas",
		IsClientStream: false,
		IsServerStream: true,
	}

	handlerChamado := false
	dummyHandler := func(_ any, _ grpc.ServerStream) error {
		handlerChamado = true
		return nil
	}

	// 9.1 Stream sem token é rejeitado
	streamSemAuth := &mockServerStream{ctx: context.Background()}
	err := interceptor(nil, streamSemAuth, info, dummyHandler)
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.Unauthenticated, st.Code())
	assert.False(t, handlerChamado)

	// 9.2 Stream com token inválido é rejeitado
	mdInvalido := metadata.Pairs("x-service-token", "token-falso")
	streamInvalido := &mockServerStream{ctx: metadata.NewIncomingContext(context.Background(), mdInvalido)}
	err = interceptor(nil, streamInvalido, info, dummyHandler)
	require.Error(t, err)
	st, _ = status.FromError(err)
	assert.Equal(t, codes.Unauthenticated, st.Code())
	assert.False(t, handlerChamado)

	// 9.3 Stream com token correto é autorizado
	mdValido := metadata.Pairs("x-service-token", chaveTesteValida)
	streamValido := &mockServerStream{ctx: metadata.NewIncomingContext(context.Background(), mdValido)}
	err = interceptor(nil, streamValido, info, dummyHandler)
	require.NoError(t, err)
	assert.True(t, handlerChamado)
}
