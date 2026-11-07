package resiliencia

import (
	"context"
	"sync"
	"time"
)

// LimitadorTaxa implementa espaçamento mínimo entre chamadas concorrentes
// para respeitar a política de uso de APIs externas (ex: máximo de 1 req/s do Nominatim).
type LimitadorTaxa struct {
	mu            sync.Mutex
	intervalo     time.Duration
	proximaJanela time.Time
}

// NovoLimitadorTaxa instancia um limitador com intervalo mínimo entre requisições.
// Se intervalo <= 0, opera em modo bypass (sem espera), ideal para testes herméticos rápidos.
func NovoLimitadorTaxa(intervalo time.Duration) *LimitadorTaxa {
	return &LimitadorTaxa{
		intervalo: intervalo,
	}
}

// Aguardar calcula o atraso necessário para respeitar o intervalo desde a última chamada
// e aguarda de forma cooperativa, respeitando o cancelamento do context.
func (l *LimitadorTaxa) Aguardar(ctx context.Context) error {
	if l == nil || l.intervalo <= 0 {
		return nil
	}

	l.mu.Lock()
	agora := time.Now()
	var espera time.Duration

	if l.proximaJanela.After(agora) {
		espera = l.proximaJanela.Sub(agora)
		l.proximaJanela = l.proximaJanela.Add(l.intervalo)
	} else {
		l.proximaJanela = agora.Add(l.intervalo)
	}
	l.mu.Unlock()

	if espera <= 0 {
		return nil
	}

	timer := time.NewTimer(espera)
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Intervalo retorna a duração configurada entre chamadas.
func (l *LimitadorTaxa) Intervalo() time.Duration {
	if l == nil {
		return 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.intervalo
}
