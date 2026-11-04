package alerta

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
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
	BuscarPorChaveIdempotencia(ctx context.Context, chave string) (Alerta, error)
	ListarAlertas(ctx context.Context) ([]Alerta, error)
}

// RepositorioOutbox gerencia as tarefas do Transactional Outbox para processamento assíncrono.
type RepositorioOutbox interface {
	SalvarAlertaComOutbox(ctx context.Context, a Alerta, t TarefaOutbox) error
	BuscarTarefasPendentes(ctx context.Context, limite int) ([]TarefaOutbox, error)
	MarcarTarefaPublicada(ctx context.Context, id string) error
	MarcarTarefaConcluida(ctx context.Context, id string) error
	IncrementarFalhaTarefa(ctx context.Context, id string, erroMsg string, proximaTentativa time.Time) error
	BuscarTarefaPorID(ctx context.Context, id string) (TarefaOutbox, error)
	BuscarTarefaPorAlertaID(ctx context.Context, alertaID string) (TarefaOutbox, error)
}

// Repositorio reúne as interfaces de armazenamento necessárias pelo serviço.
type Repositorio interface {
	RepositorioContatos
	RepositorioAlertas
	RepositorioOutbox
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
// validar → verificar idempotência → registrar → notificar (+ geocodificar em paralelo) → atualizar.
type Servico struct {
	repositorio            Repositorio
	despachante            DespachanteNotificacao
	geocodificador         Geocodificador // opcional: pode ser nil
	timeoutPorAlerta       time.Duration
	bloqueiosChave         sync.Map // chaveIdempotencia -> *sync.Mutex (concorrência local)
	bloqueiosProcessamento sync.Map // alertaID -> *sync.Mutex (concorrência de processamento)
	modoAssincrono         bool
	notificadorOutbox      func()
}

// NovoServico monta o serviço com suas dependências (injeção de dependência).
// geo pode ser nil quando a geocodificação estiver desativada.
func NovoServico(repo Repositorio, desp DespachanteNotificacao, geo Geocodificador, timeout time.Duration) *Servico {
	return &Servico{
		repositorio:      repo,
		despachante:      desp,
		geocodificador:   geo,
		timeoutPorAlerta: timeout,
	}
}

// AtivarModoAssincrono ativa o Transactional Outbox e conecta o gatilho do Relay.
func (s *Servico) AtivarModoAssincrono(notificador func()) {
	s.modoAssincrono = true
	s.notificadorOutbox = notificador
}

// ModoAssincrono informa se o processamento por filas de mensagens está habilitado.
func (s *Servico) ModoAssincrono() bool {
	return s.modoAssincrono
}

// obterTrava garante exclusão mútua em nível de chave de idempotência para requisições concorrentes.
func (s *Servico) obterTrava(chave string) func() {
	if chave == "" {
		return func() {}
	}
	val, _ := s.bloqueiosChave.LoadOrStore(chave, &sync.Mutex{})
	mu := val.(*sync.Mutex)
	mu.Lock()
	return func() {
		mu.Unlock()
	}
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

	// 1. Idempotência e Concorrência: se informada chave, sincroniza e verifica duplicidade
	if pedido.ChaveIdempotencia != "" {
		liberar := s.obterTrava(pedido.ChaveIdempotencia)
		defer liberar()

		existente, err := s.repositorio.BuscarPorChaveIdempotencia(ctx, pedido.ChaveIdempotencia)
		if err == nil {
			// Requisição repetida: se os dados diferem, é conflito (HTTP 409)
			if existente.HashRequisicao != "" && existente.HashRequisicao != pedido.HashConteudo() {
				return Alerta{}, ErrConflitoIdempotencia
			}
			// Se os dados são os mesmos, devolve o alerta original sem disparar notificações novamente
			existente.Replay = true
			return existente, nil
		}
		if !errors.Is(err, ErrNaoEncontrado) {
			return Alerta{}, fmt.Errorf("verificar chave de idempotência: %w", err)
		}
	}

	id, err := novoID()
	if err != nil {
		return Alerta{}, fmt.Errorf("gerar id do alerta: %w", err)
	}

	agora := time.Now().UTC()
	a := Alerta{
		ID:                id,
		ChaveIdempotencia: pedido.ChaveIdempotencia,
		HashRequisicao:    pedido.HashConteudo(),
		UsuariaID:         pedido.UsuariaID,
		Nivel:             pedido.Nivel,
		Mensagem:          pedido.Mensagem,
		Local:             pedido.Local,
		Silencioso:        pedido.Silencioso,
		Status:            StatusNotificando,
		CriadoEm:          agora,
		AtualizadoEm:      agora,
		Notificacoes:      []ResultadoEnvio{},
	}

	destinatarios, instituicoes, err := s.montarDestinatarios(ctx, pedido)
	if err != nil {
		return Alerta{}, err
	}
	a.InstituicoesAlvo = instituicoes

	// FLUXO ASSÍNCRONO COM TRANSACTIONAL OUTBOX (Semana 12 CIT7219)
	if s.modoAssincrono {
		a.Status = StatusProcessando

		tarefaID, err := novoID()
		if err != nil {
			return Alerta{}, fmt.Errorf("gerar id da tarefa outbox: %w", err)
		}

		evento := EventoAlertaCriado{
			TarefaID:  "tarefa-" + tarefaID,
			AlertaID:  a.ID,
			UsuariaID: a.UsuariaID,
			Nivel:     string(a.Nivel),
			CriadoEm:  agora.Format(time.RFC3339Nano),
		}
		payloadBytes, err := json.Marshal(evento)
		if err != nil {
			return Alerta{}, fmt.Errorf("serializar evento outbox: %w", err)
		}

		tarefa := TarefaOutbox{
			ID:                 "tarefa-" + tarefaID,
			AlertaID:           a.ID,
			Topico:             "alertas.notificar",
			PayloadJSON:        string(payloadBytes),
			Estado:             EstadoTarefaPendente,
			MaxTentativas:      5,
			ProximaTentativaEm: agora,
			CriadoEm:           agora,
			AtualizadoEm:       agora,
		}

		// Gravação atômica: Alerta + Tarefa Outbox no mesmo commit do SQLite
		if err := s.repositorio.SalvarAlertaComOutbox(ctx, a, tarefa); err != nil {
			return Alerta{}, fmt.Errorf("registrar alerta e outbox: %w", err)
		}

		// Geocodificação em background se disponível
		if s.geocodificador != nil {
			go func(alertaID string, loc Localizacao) {
				ctxGeo, cancel := context.WithTimeout(context.Background(), s.timeoutPorAlerta)
				defer cancel()
				end := s.geocodificar(ctxGeo, loc)
				if end != "" {
					alt, err := s.repositorio.BuscarAlerta(context.Background(), alertaID)
					if err == nil {
						alt.EnderecoAproximado = end
						_ = s.repositorio.AtualizarAlerta(context.Background(), alt)
					}
				}
			}(a.ID, pedido.Local)
		}

		if s.notificadorOutbox != nil {
			s.notificadorOutbox()
		}

		return a, nil
	}

	// FLUXO SÍNCRONO CONCORRENTE (compatibilidade de testes e execução sem broker)
	if err := s.repositorio.SalvarAlerta(ctx, a); err != nil {
		return Alerta{}, fmt.Errorf("registrar alerta: %w", err)
	}

	ctxEnvio, cancelar := context.WithTimeout(ctx, s.timeoutPorAlerta)
	defer cancelar()

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

// ProcessarTarefaNotificacao é invocado pelo worker de mensageria para executar as notificações simuladas.
// Implementa idempotência no consumidor: se o alerta já estiver concluído, evita redespacho.
func (s *Servico) ProcessarTarefaNotificacao(ctx context.Context, alertaID string) error {
	val, _ := s.bloqueiosProcessamento.LoadOrStore(alertaID, &sync.Mutex{})
	mtx := val.(*sync.Mutex)
	mtx.Lock()
	defer mtx.Unlock()

	a, err := s.repositorio.BuscarAlerta(ctx, alertaID)
	if err != nil {
		return fmt.Errorf("buscar alerta para notificar: %w", err)
	}

	// Idempotência no Consumidor: se já estiver concluído, confirma sem reenviar
	if a.Status == StatusConcluido {
		return nil
	}

	pedido := PedidoAlerta{
		UsuariaID: a.UsuariaID,
		Nivel:     a.Nivel,
		Mensagem:  a.Mensagem,
	}

	destinatarios, _, err := s.montarDestinatarios(ctx, pedido)
	if err != nil {
		return fmt.Errorf("montar destinatarios: %w", err)
	}

	ctxEnvio, cancelar := context.WithTimeout(ctx, s.timeoutPorAlerta)
	defer cancelar()

	notificacoes := s.despachante.Despachar(ctxEnvio, destinatarios, s.textoAlerta(pedido))
	a.Notificacoes = notificacoes
	a.Status = StatusConcluido
	a.AtualizadoEm = time.Now().UTC()

	if err := s.repositorio.AtualizarAlerta(ctx, a); err != nil {
		return fmt.Errorf("atualizar alerta com notificacoes: %w", err)
	}
	return nil
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
