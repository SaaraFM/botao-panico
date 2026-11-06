package armazenamento

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/plataforma-mulher/botao-panico/internal/alerta"
)

// Memoria é um repositório em memória, seguro para uso concorrente
// (sync.RWMutex). Útil em testes; a aplicação usa o SQLite.
// Guarda VALORES e devolve cópias profundas: ninguém fora do repositório
// consegue alterar um alerta armazenado sem passar pelo mutex.
type Memoria struct {
	mu           sync.RWMutex
	alertas      map[string]alerta.Alerta
	contatos     map[string][]alerta.ContatoEmergencia // chave: usuaria_id
	instituicoes []alerta.InstituicaoApoio
	outbox       map[string]alerta.TarefaOutbox
	auditoria    []alerta.RegistroAuditoria
}

// NovaMemoria cria o repositório já populado com dados de exemplo.
func NovaMemoria() *Memoria {
	return &Memoria{
		alertas:      make(map[string]alerta.Alerta),
		contatos:     map[string][]alerta.ContatoEmergencia{UsuariaDemo: contatosExemplo()},
		instituicoes: instituicoesExemplo(),
		outbox:       make(map[string]alerta.TarefaOutbox),
		auditoria:    make([]alerta.RegistroAuditoria, 0),
	}
}

// SalvarAlerta grava um alerta novo.
func (m *Memoria) SalvarAlerta(_ context.Context, a alerta.Alerta) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, existe := m.alertas[a.ID]; existe {
		return fmt.Errorf("%w: alerta %s já existe", alerta.ErrEscrita, a.ID)
	}
	m.alertas[a.ID] = a.Copia()
	return nil
}

// AtualizarAlerta substitui um alerta existente.
func (m *Memoria) AtualizarAlerta(_ context.Context, a alerta.Alerta) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, existe := m.alertas[a.ID]; !existe {
		return alerta.ErrNaoEncontrado
	}
	m.alertas[a.ID] = a.Copia()
	return nil
}

// BuscarAlerta devolve uma cópia do alerta ou ErrNaoEncontrado.
func (m *Memoria) BuscarAlerta(_ context.Context, id string) (alerta.Alerta, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	a, ok := m.alertas[id]
	if !ok {
		return alerta.Alerta{}, alerta.ErrNaoEncontrado
	}
	return a.Copia(), nil
}

// BuscarPorChaveIdempotencia devolve uma cópia do alerta que possui a chave ou ErrNaoEncontrado.
func (m *Memoria) BuscarPorChaveIdempotencia(_ context.Context, chave string) (alerta.Alerta, error) {
	if chave == "" {
		return alerta.Alerta{}, alerta.ErrNaoEncontrado
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, a := range m.alertas {
		if a.ChaveIdempotencia == chave {
			return a.Copia(), nil
		}
	}
	return alerta.Alerta{}, alerta.ErrNaoEncontrado
}

// ListarAlertas devolve cópias de todos os alertas, do mais recente ao mais antigo.
func (m *Memoria) ListarAlertas(_ context.Context) ([]alerta.Alerta, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	lista := make([]alerta.Alerta, 0, len(m.alertas))
	for _, a := range m.alertas {
		lista = append(lista, a.Copia())
	}
	sort.Slice(lista, func(i, j int) bool { return lista[i].CriadoEm.After(lista[j].CriadoEm) })
	return lista, nil
}

// ContatosDaUsuaria devolve uma cópia dos contatos de emergência da usuária.
func (m *Memoria) ContatosDaUsuaria(_ context.Context, usuariaID string) ([]alerta.ContatoEmergencia, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return append([]alerta.ContatoEmergencia{}, m.contatos[usuariaID]...), nil
}

// Instituicoes devolve uma cópia da rede institucional de proteção.
func (m *Memoria) Instituicoes(_ context.Context) ([]alerta.InstituicaoApoio, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return append([]alerta.InstituicaoApoio{}, m.instituicoes...), nil
}

// SalvarAlertaComOutbox grava alerta e tarefa outbox de forma atômica em memória.
func (m *Memoria) SalvarAlertaComOutbox(_ context.Context, a alerta.Alerta, t alerta.TarefaOutbox) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, existe := m.alertas[a.ID]; existe {
		return fmt.Errorf("%w: alerta %s já existe", alerta.ErrEscrita, a.ID)
	}
	m.alertas[a.ID] = a.Copia()
	m.outbox[t.ID] = t
	return nil
}

// BuscarTarefasPendentes retorna tarefas pendentes em memória.
func (m *Memoria) BuscarTarefasPendentes(_ context.Context, limite int) ([]alerta.TarefaOutbox, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	agora := time.Now()
	var lista []alerta.TarefaOutbox
	for _, t := range m.outbox {
		if t.Estado == alerta.EstadoTarefaPendente && !t.ProximaTentativaEm.After(agora) {
			lista = append(lista, t)
			if limite > 0 && len(lista) >= limite {
				break
			}
		}
	}
	return lista, nil
}

// MarcarTarefaPublicada atualiza o estado para 'publicado'.
func (m *Memoria) MarcarTarefaPublicada(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, existe := m.outbox[id]
	if !existe {
		return alerta.ErrNaoEncontrado
	}
	t.Estado = alerta.EstadoTarefaPublicado
	t.AtualizadoEm = time.Now()
	m.outbox[id] = t
	return nil
}

// MarcarTarefaConcluida atualiza o estado para 'concluido'.
func (m *Memoria) MarcarTarefaConcluida(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, existe := m.outbox[id]
	if !existe {
		return alerta.ErrNaoEncontrado
	}
	t.Estado = alerta.EstadoTarefaConcluido
	t.AtualizadoEm = time.Now()
	m.outbox[id] = t
	return nil
}

// IncrementarFalhaTarefa registra tentativa falha em memória.
func (m *Memoria) IncrementarFalhaTarefa(_ context.Context, id string, erroMsg string, proximaTentativa time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, existe := m.outbox[id]
	if !existe {
		return alerta.ErrNaoEncontrado
	}
	t.Tentativas++
	t.ErroUltimo = erroMsg
	t.ProximaTentativaEm = proximaTentativa
	t.AtualizadoEm = time.Now()
	if t.Tentativas >= t.MaxTentativas {
		t.Estado = alerta.EstadoTarefaFalha
	} else {
		t.Estado = alerta.EstadoTarefaPendente
	}
	m.outbox[id] = t
	return nil
}

// BuscarTarefaPorID consulta uma tarefa em memória por ID.
func (m *Memoria) BuscarTarefaPorID(_ context.Context, id string) (alerta.TarefaOutbox, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	t, existe := m.outbox[id]
	if !existe {
		return alerta.TarefaOutbox{}, alerta.ErrNaoEncontrado
	}
	return t, nil
}

// BuscarTarefaPorAlertaID consulta a tarefa outbox vinculada ao alerta em memória.
func (m *Memoria) BuscarTarefaPorAlertaID(_ context.Context, alertaID string) (alerta.TarefaOutbox, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, t := range m.outbox {
		if t.AlertaID == alertaID {
			return t, nil
		}
	}
	return alerta.TarefaOutbox{}, alerta.ErrNaoEncontrado
}

// SalvarAuditoria armazena um registro de auditoria em memória.
func (m *Memoria) SalvarAuditoria(_ context.Context, reg alerta.RegistroAuditoria) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if reg.RecebidoEm.IsZero() {
		reg.RecebidoEm = time.Now().UTC()
	}
	m.auditoria = append(m.auditoria, reg)
	return nil
}

// ListarAuditoria retorna os eventos de auditoria armazenados.
func (m *Memoria) ListarAuditoria(_ context.Context, limite int) ([]alerta.RegistroAuditoria, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if limite <= 0 || limite > len(m.auditoria) {
		limite = len(m.auditoria)
	}
	resultado := make([]alerta.RegistroAuditoria, limite)
	for i := 0; i < limite; i++ {
		// Ordem decrescente (mais recentes primeiro)
		resultado[i] = m.auditoria[len(m.auditoria)-1-i]
	}
	return resultado, nil
}
