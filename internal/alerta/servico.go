package alerta

import (
	"context"
	"crypto/rand"
	"fmt"
	"log/slog"
	"time"
)

// RepositorioContatos é a parte do armazenamento que lê a rede de apoio.
type RepositorioContatos interface {
	ContatosDaUsuaria(ctx context.Context, usuariaID string) ([]ContatoEmergencia, error)
	Instituicoes(ctx context.Context) ([]InstituicaoApoio, error)
}

// RepositorioAlertas persiste os alertas. Trabalha com VALORES (não ponteiros):
// cada chamada recebe/devolve uma cópia, evitando compartilhar memória mutável
// entre goroutines (a causa da condição de corrida da versão anterior).
type RepositorioAlertas interface {
	SalvarAlerta(ctx context.Context, a Alerta) error
	AtualizarAlerta(ctx context.Context, a Alerta) error
	BuscarAlerta(ctx context.Context, id string) (Alerta, error)
	ListarAlertas(ctx context.Context) ([]Alerta, error)
}

// Repositorio reúne as duas interfaces acima; é o que o serviço precisa.
type Repositorio interface {
	RepositorioContatos
	RepositorioAlertas
}

// DespachanteNotificacao envia as notificações (implementado com concorrência).
type DespachanteNotificacao interface {
	Despachar(ctx context.Context, destinatarios []Destinatario, mensagem string) []ResultadoEnvio
}

// Geocodificador traduz coordenadas em um endereço aproximado (API externa).
type Geocodificador interface {
	EnderecoAproximado(ctx context.Context, local Localizacao) (string, error)
}

// Servico orquestra o fluxo completo do botão de pânico:
// validar → registrar → notificar (+ geocodificar em paralelo) → atualizar.
type Servico struct {
	repositorio      Repositorio
	despachante      DespachanteNotificacao
	geocodificador   Geocodificador // opcional: pode ser nil
	timeoutPorAlerta time.Duration
}

// NovoServico monta o serviço com suas dependências (injeção de dependência).
// geo pode ser nil quando a geocodificação estiver desativada.
func NovoServico(repo Repositorio, desp DespachanteNotificacao, geo Geocodificador, timeout time.Duration) *Servico {
	return &Servico{repositorio: repo, despachante: desp, geocodificador: geo, timeoutPorAlerta: timeout}
}

// Acionar processa um pedido de botão de pânico de ponta a ponta.
//
// O alerta é montado numa variável LOCAL desta goroutine e só chega ao
// repositório como cópia; leituras concorrentes nunca veem um objeto sendo
// modificado no meio do processamento.
func (s *Servico) Acionar(ctx context.Context, pedido PedidoAlerta) (Alerta, error) {
	if err := pedido.Validar(); err != nil {
		return Alerta{}, err
	}

	id, err := novoID()
	if err != nil {
		return Alerta{}, fmt.Errorf("gerar id do alerta: %w", err)
	}

	agora := time.Now().UTC()
	a := Alerta{
		ID:           id,
		UsuariaID:    pedido.UsuariaID,
		Nivel:        pedido.Nivel,
		Mensagem:     pedido.Mensagem,
		Local:        pedido.Local,
		Silencioso:   pedido.Silencioso,
		Status:       StatusNotificando,
		CriadoEm:     agora,
		AtualizadoEm: agora,
		Notificacoes: []ResultadoEnvio{},
	}

	destinatarios, instituicoes, err := s.montarDestinatarios(ctx, pedido)
	if err != nil {
		return Alerta{}, err
	}
	a.InstituicoesAlvo = instituicoes

	// Registro do alerta ANTES de notificar: se algo falhar depois,
	// o acionamento já está gravado.
	if err := s.repositorio.SalvarAlerta(ctx, a); err != nil {
		return Alerta{}, fmt.Errorf("registrar alerta: %w", err)
	}

	ctxEnvio, cancelar := context.WithTimeout(ctx, s.timeoutPorAlerta)
	defer cancelar()

	// A geocodificação roda em paralelo às notificações (goroutine + canal),
	// mas Acionar aguarda seu resultado. A chamada respeita timeoutPorAlerta;
	// falha ou timeout da API Nominatim gera endereço vazio e não impede
	// a conclusão do alerta. Silencioso é preservado como preferência da
	// interface e não modifica o despacho de notificações.
	enderecoCh := make(chan string, 1)
	go func() { enderecoCh <- s.geocodificar(ctxEnvio, pedido.Local) }()

	a.Notificacoes = s.despachante.Despachar(ctxEnvio, destinatarios, s.textoAlerta(pedido))
	a.EnderecoAproximado = <-enderecoCh
	a.Status = StatusConcluido
	a.AtualizadoEm = time.Now().UTC()

	if err := s.repositorio.AtualizarAlerta(ctx, a); err != nil {
		return Alerta{}, fmt.Errorf("atualizar alerta %s: %w", a.ID, err)
	}
	return a, nil
}

// Buscar devolve um alerta pelo ID (ErrNaoEncontrado se não existir).
func (s *Servico) Buscar(ctx context.Context, id string) (Alerta, error) {
	return s.repositorio.BuscarAlerta(ctx, id)
}

// Listar devolve todos os alertas registrados.
func (s *Servico) Listar(ctx context.Context) ([]Alerta, error) {
	return s.repositorio.ListarAlertas(ctx)
}

// Instituicoes devolve a rede de proteção cadastrada.
func (s *Servico) Instituicoes(ctx context.Context) ([]InstituicaoApoio, error) {
	return s.repositorio.Instituicoes(ctx)
}

// geocodificar consulta o endereço aproximado; em caso de falha registra no
// log e devolve "" — o alerta é concluído normalmente só com as coordenadas.
func (s *Servico) geocodificar(ctx context.Context, local Localizacao) string {
	if s.geocodificador == nil {
		return ""
	}
	endereco, err := s.geocodificador.EnderecoAproximado(ctx, local)
	if err != nil {
		slog.Warn("geocodificação indisponível; alerta segue apenas com coordenadas", "erro", err)
		return ""
	}
	return endereco
}

// montarDestinatarios: contatos pessoais sempre; instituições de proteção
// quando o risco é alto ou crítico.
func (s *Servico) montarDestinatarios(ctx context.Context, pedido PedidoAlerta) ([]Destinatario, []InstituicaoApoio, error) {
	contatos, err := s.repositorio.ContatosDaUsuaria(ctx, pedido.UsuariaID)
	if err != nil {
		return nil, nil, fmt.Errorf("carregar contatos: %w", err)
	}

	destinatarios := make([]Destinatario, 0, len(contatos))
	for _, c := range contatos {
		destinatarios = append(destinatarios, Destinatario{Nome: c.Nome, Telefone: c.Telefone, Email: c.Email})
	}

	if !pedido.ExigeInstituicoes() {
		return destinatarios, nil, nil
	}

	instituicoes, err := s.repositorio.Instituicoes(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("carregar instituições: %w", err)
	}
	for _, inst := range instituicoes {
		destinatarios = append(destinatarios, Destinatario{Nome: inst.Nome, Telefone: inst.Telefone})
	}
	return destinatarios, instituicoes, nil
}

// textoAlerta compõe a mensagem enviada aos destinatários.
func (s *Servico) textoAlerta(pedido PedidoAlerta) string {
	texto := fmt.Sprintf(
		"ALERTA DE PÂNICO (risco %s)! Localização: https://maps.google.com/?q=%f,%f",
		pedido.Nivel, pedido.Local.Latitude, pedido.Local.Longitude,
	)
	if pedido.Mensagem != "" {
		texto += " — " + pedido.Mensagem
	}
	return texto
}

// novoID gera um identificador aleatório; a falha do gerador é propagada.
func novoID() (string, error) {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return fmt.Sprintf("alerta-%x", b), nil
}
