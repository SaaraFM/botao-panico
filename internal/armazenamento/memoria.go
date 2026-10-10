package armazenamento

import (
	"context"
	"fmt"
	"sort"
	"sync"

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
}

// NovaMemoria cria o repositório já populado com dados de exemplo.
func NovaMemoria() *Memoria {
	return &Memoria{
		alertas:      make(map[string]alerta.Alerta),
		contatos:     map[string][]alerta.ContatoEmergencia{UsuariaDemo: contatosExemplo()},
		instituicoes: instituicoesExemplo(),
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
