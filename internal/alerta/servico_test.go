package alerta_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/plataforma-mulher/botao-panico/internal/alerta"
	"github.com/plataforma-mulher/botao-panico/internal/armazenamento"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var localValido = alerta.Localizacao{Latitude: -28.9346, Longitude: -49.4957}

// despachanteFalso é um mock: registra os destinatários e devolve sucesso.
type despachanteFalso struct {
	mu            sync.Mutex
	destinatarios []alerta.Destinatario
	liberar       chan struct{} // se não for nil, bloqueia até ser fechado
	iniciou       chan struct{}
	umaVez        sync.Once
	enviosTotais  int64
}

func (d *despachanteFalso) Despachar(_ context.Context, destinatarios []alerta.Destinatario, _ string) []alerta.ResultadoEnvio {
	atomic.AddInt64(&d.enviosTotais, 1)
	if d.iniciou != nil {
		d.umaVez.Do(func() { close(d.iniciou) })
	}
	if d.liberar != nil {
		<-d.liberar
	}
	d.mu.Lock()
	d.destinatarios = destinatarios
	d.mu.Unlock()
	resultados := make([]alerta.ResultadoEnvio, len(destinatarios))
	for i, dest := range destinatarios {
		resultados[i] = alerta.ResultadoEnvio{Destino: dest.Nome, Canal: "sms", Sucesso: true}
	}
	return resultados
}

type geoFalso struct {
	endereco string
	err      error
}

func (g geoFalso) EnderecoAproximado(context.Context, alerta.Localizacao) (string, error) {
	return g.endereco, g.err
}

// repoComFalha simula banco quebrado em operações específicas.
type repoComFalha struct {
	*armazenamento.Memoria
	falhaSalvar   error
	falhaContatos error
}

func (r repoComFalha) SalvarAlerta(ctx context.Context, a alerta.Alerta) error {
	if r.falhaSalvar != nil {
		return r.falhaSalvar
	}
	return r.Memoria.SalvarAlerta(ctx, a)
}

func (r repoComFalha) ContatosDaUsuaria(ctx context.Context, id string) ([]alerta.ContatoEmergencia, error) {
	if r.falhaContatos != nil {
		return nil, r.falhaContatos
	}
	return r.Memoria.ContatosDaUsuaria(ctx, id)
}

func TestAcionar_RiscoAltoNotificaContatosEInstituicoes(t *testing.T) {
	desp := &despachanteFalso{}
	servico := alerta.NovoServico(armazenamento.NovaMemoria(), desp, nil, 5*time.Second)

	a, err := servico.Acionar(context.Background(), alerta.PedidoAlerta{
		UsuariaID: armazenamento.UsuariaDemo, Nivel: alerta.RiscoAlto, Local: localValido,
	})

	require.NoError(t, err)
	assert.Equal(t, alerta.StatusConcluido, a.Status)
	assert.Len(t, desp.destinatarios, 7, "3 contatos + 4 instituições")
	assert.Len(t, a.InstituicoesAlvo, 4)
	assert.Len(t, a.Notificacoes, 7)
}

func TestAcionar_RiscoBaixoNotificaSoContatos(t *testing.T) {
	desp := &despachanteFalso{}
	servico := alerta.NovoServico(armazenamento.NovaMemoria(), desp, nil, 5*time.Second)

	a, err := servico.Acionar(context.Background(), alerta.PedidoAlerta{
		UsuariaID: armazenamento.UsuariaDemo, Nivel: alerta.RiscoBaixo, Local: localValido,
	})

	require.NoError(t, err)
	assert.Len(t, desp.destinatarios, 3)
	assert.Empty(t, a.InstituicoesAlvo)
}

func TestAcionar_PedidoInvalidoNaoGravaNada(t *testing.T) {
	repo := armazenamento.NovaMemoria()
	servico := alerta.NovoServico(repo, &despachanteFalso{}, nil, time.Second)

	_, err := servico.Acionar(context.Background(), alerta.PedidoAlerta{})

	var errosVal *alerta.ErrosValidacao
	require.ErrorAs(t, err, &errosVal)
	lista, err := repo.ListarAlertas(context.Background())
	require.NoError(t, err)
	assert.Empty(t, lista)
}

func TestAcionar_GuardaEnderecoDaGeocodificacao(t *testing.T) {
	repo := armazenamento.NovaMemoria()
	geo := geoFalso{endereco: "Avenida Getúlio Vargas, Araranguá, SC"}
	servico := alerta.NovoServico(repo, &despachanteFalso{}, geo, time.Second)

	a, err := servico.Acionar(context.Background(), alerta.PedidoAlerta{
		UsuariaID: armazenamento.UsuariaDemo, Nivel: alerta.RiscoBaixo, Local: localValido,
	})

	require.NoError(t, err)
	assert.Equal(t, geo.endereco, a.EnderecoAproximado)
	assert.Equal(t, localValido, a.Local, "coordenadas originais são preservadas")
	salvo, err := repo.BuscarAlerta(context.Background(), a.ID)
	require.NoError(t, err)
	assert.Equal(t, geo.endereco, salvo.EnderecoAproximado)
}

func TestAcionar_FalhaNaGeocodificacaoNaoImpedeAlerta(t *testing.T) {
	geo := geoFalso{err: errors.New("serviço externo indisponível")}
	servico := alerta.NovoServico(armazenamento.NovaMemoria(), &despachanteFalso{}, geo, time.Second)

	a, err := servico.Acionar(context.Background(), alerta.PedidoAlerta{
		UsuariaID: armazenamento.UsuariaDemo, Nivel: alerta.RiscoCritico, Local: localValido,
	})

	require.NoError(t, err, "o pedido de socorro não pode depender da API auxiliar")
	assert.Equal(t, alerta.StatusConcluido, a.Status)
	assert.Empty(t, a.EnderecoAproximado)
	assert.Equal(t, localValido, a.Local)
}

func TestAcionar_IdempotenciaMesmoConteudoRetornaOriginal(t *testing.T) {
	repo := armazenamento.NovaMemoria()
	desp := &despachanteFalso{}
	servico := alerta.NovoServico(repo, desp, nil, 5*time.Second)

	pedido := alerta.PedidoAlerta{
		ChaveIdempotencia: "chave-teste-1234",
		UsuariaID:         armazenamento.UsuariaDemo,
		Nivel:             alerta.RiscoAlto,
		Local:             localValido,
		Mensagem:          "socorro teste",
	}

	primeiro, err := servico.Acionar(context.Background(), pedido)
	require.NoError(t, err)
	assert.False(t, primeiro.Replay)
	assert.Equal(t, int64(1), atomic.LoadInt64(&desp.enviosTotais))

	// Reenviar a mesma chave com exatamente o mesmo pedido:
	segundo, err := servico.Acionar(context.Background(), pedido)
	require.NoError(t, err)
	assert.True(t, segundo.Replay)
	assert.Equal(t, primeiro.ID, segundo.ID)
	assert.Equal(t, primeiro.CriadoEm, segundo.CriadoEm)
	// Notificações NÃO foram redespachadas:
	assert.Equal(t, int64(1), atomic.LoadInt64(&desp.enviosTotais))
}

func TestAcionar_IdempotenciaConteudoDiferenteRetornaConflito(t *testing.T) {
	repo := armazenamento.NovaMemoria()
	desp := &despachanteFalso{}
	servico := alerta.NovoServico(repo, desp, nil, 5*time.Second)

	pedido1 := alerta.PedidoAlerta{
		ChaveIdempotencia: "chave-conflito-123",
		UsuariaID:         armazenamento.UsuariaDemo,
		Nivel:             alerta.RiscoBaixo,
		Local:             localValido,
		Mensagem:          "mensagem original",
	}

	_, err := servico.Acionar(context.Background(), pedido1)
	require.NoError(t, err)

	pedido2 := alerta.PedidoAlerta{
		ChaveIdempotencia: "chave-conflito-123",
		UsuariaID:         armazenamento.UsuariaDemo,
		Nivel:             alerta.RiscoCritico, // nível alterado!
		Local:             localValido,
		Mensagem:          "mensagem diferente",
	}

	_, err = servico.Acionar(context.Background(), pedido2)
	require.ErrorIs(t, err, alerta.ErrConflitoIdempotencia)
}

func TestAcionar_IdempotenciaConcorrenteMesmaChave(t *testing.T) {
	repo := armazenamento.NovaMemoria()
	desp := &despachanteFalso{}
	servico := alerta.NovoServico(repo, desp, nil, 5*time.Second)

	pedido := alerta.PedidoAlerta{
		ChaveIdempotencia: "chave-concorrente-abc",
		UsuariaID:         armazenamento.UsuariaDemo,
		Nivel:             alerta.RiscoAlto,
		Local:             localValido,
	}

	var wg sync.WaitGroup
	resultados := make([]alerta.Alerta, 10)
	erros := make([]error, 10)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			resultados[idx], erros[idx] = servico.Acionar(context.Background(), pedido)
		}(i)
	}
	wg.Wait()

	for i := 0; i < 10; i++ {
		assert.NoError(t, erros[i])
		assert.Equal(t, resultados[0].ID, resultados[i].ID)
	}

	// O despachante deve ter sido acionado apenas UMA vez
	assert.Equal(t, int64(1), atomic.LoadInt64(&desp.enviosTotais))
}

func TestAcionar_PropagaErrosDeInfraestrutura(t *testing.T) {
	casos := []struct {
		nome     string
		repo     repoComFalha
		esperado error
	}{
		{"falha ao gravar", repoComFalha{Memoria: armazenamento.NovaMemoria(), falhaSalvar: alerta.ErrEscrita}, alerta.ErrEscrita},
		{"banco indisponível", repoComFalha{Memoria: armazenamento.NovaMemoria(), falhaContatos: alerta.ErrBancoIndisponivel}, alerta.ErrBancoIndisponivel},
	}
	for _, c := range casos {
		t.Run(c.nome, func(t *testing.T) {
			servico := alerta.NovoServico(c.repo, &despachanteFalso{}, nil, time.Second)
			_, err := servico.Acionar(context.Background(), alerta.PedidoAlerta{
				UsuariaID: armazenamento.UsuariaDemo, Nivel: alerta.RiscoAlto, Local: localValido,
			})
			assert.ErrorIs(t, err, c.esperado)
		})
	}
}

func TestAcionar_LeituraConcorrenteDuranteProcessamento(t *testing.T) {
	repo := armazenamento.NovaMemoria()
	desp := &despachanteFalso{liberar: make(chan struct{}), iniciou: make(chan struct{})}
	servico := alerta.NovoServico(repo, desp, geoFalso{endereco: "Araranguá"}, 5*time.Second)
	ctx := context.Background()

	resultado := make(chan alerta.Alerta, 1)
	go func() {
		a, err := servico.Acionar(ctx, alerta.PedidoAlerta{
			UsuariaID: armazenamento.UsuariaDemo, Nivel: alerta.RiscoAlto, Local: localValido,
		})
		assert.NoError(t, err)
		resultado <- a
	}()

	<-desp.iniciou // o alerta já foi gravado e está "notificando"
	lista, err := servico.Listar(ctx)
	require.NoError(t, err)
	require.Len(t, lista, 1)
	id := lista[0].ID
	assert.Equal(t, alerta.StatusNotificando, lista[0].Status)

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				a, err := servico.Buscar(ctx, id)
				if assert.NoError(t, err) {
					_ = len(a.Notificacoes) // lê os campos que o processamento altera
					_ = a.Status
					a.Notificacoes = append(a.Notificacoes, alerta.ResultadoEnvio{}) // mutar a cópia não afeta o repositório
				}
				_, _ = servico.Listar(ctx)
			}
		}()
	}

	close(desp.liberar) // conclui as notificações enquanto as leituras acontecem
	wg.Wait()
	final := <-resultado

	salvo, err := servico.Buscar(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, alerta.StatusConcluido, salvo.Status)
	assert.Len(t, salvo.Notificacoes, 7, "as mutações nas cópias não vazaram para o repositório")
	assert.Equal(t, final.Notificacoes, salvo.Notificacoes)
}

func TestAcionar_VariosAlertasSimultaneos(t *testing.T) {
	repo := armazenamento.NovaMemoria()
	servico := alerta.NovoServico(repo, &despachanteFalso{}, nil, time.Second)

	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := servico.Acionar(context.Background(), alerta.PedidoAlerta{
				UsuariaID: armazenamento.UsuariaDemo, Nivel: alerta.RiscoModerado, Local: localValido,
			})
			assert.NoError(t, err)
		}()
	}
	wg.Wait()

	lista, err := servico.Listar(context.Background())
	require.NoError(t, err)
	assert.Len(t, lista, 30)
}

func TestBuscar_Inexistente(t *testing.T) {
	servico := alerta.NovoServico(armazenamento.NovaMemoria(), &despachanteFalso{}, nil, time.Second)
	_, err := servico.Buscar(context.Background(), "nao-existe")
	assert.ErrorIs(t, err, alerta.ErrNaoEncontrado)
}
