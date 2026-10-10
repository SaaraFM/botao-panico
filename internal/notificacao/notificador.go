// Package notificacao despacha as notificações do botão de pânico para
// contatos e instituições EM PARALELO, usando goroutines, WaitGroup, Mutex
// e context.Context — os padrões de concorrência cobrados na disciplina.
package notificacao

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/plataforma-mulher/botao-panico/internal/alerta"
)

// Notificador é a interface que abstrai o canal de envio (SMS, e-mail...).
// Qualquer implementação concreta pode ser plugada no Despachante.
type Notificador interface {
	Canal() string
	Enviar(ctx context.Context, destinatario, mensagem string) error
}

// Despachante envia notificações para vários destinatários simultaneamente,
// coletando os resultados de forma segura.
type Despachante struct {
	notificadores []Notificador
}

// NovoDespachante cria o despachante com os canais disponíveis.
func NovoDespachante(notificadores ...Notificador) *Despachante {
	return &Despachante{notificadores: notificadores}
}

// Despachar dispara uma goroutine para cada combinação destinatário × canal.
// O WaitGroup garante que só retornamos quando TODAS terminarem (ou o contexto
// expirar), e o Mutex protege o slice de resultados contra escrita concorrente.
func (d *Despachante) Despachar(ctx context.Context, destinatarios []alerta.Destinatario, mensagem string) []alerta.ResultadoEnvio {
	var wg sync.WaitGroup
	var mu sync.Mutex
	resultados := make([]alerta.ResultadoEnvio, 0, len(destinatarios)*len(d.notificadores))

	for _, dest := range destinatarios {
		for _, n := range d.notificadores {
			// E-mail só faz sentido quando o destinatário tem e-mail cadastrado.
			if n.Canal() == "email" && dest.Email == "" {
				continue
			}

			wg.Add(1)
			go func(dest alerta.Destinatario, n Notificador) {
				defer wg.Done()

				inicio := time.Now()
				endereco := dest.Telefone
				if n.Canal() == "email" {
					endereco = dest.Email
				}

				err := n.Enviar(ctx, endereco, mensagem)

				resultado := alerta.ResultadoEnvio{
					Destino:   dest.Nome,
					Canal:     n.Canal(),
					Sucesso:   err == nil,
					DuracaoMs: time.Since(inicio).Milliseconds(),
					EnviadoEm: time.Now().Format(time.RFC3339),
				}
				if err != nil {
					resultado.Erro = err.Error()
				}

				mu.Lock()
				resultados = append(resultados, resultado)
				mu.Unlock()
			}(dest, n)
		}
	}

	wg.Wait()
	return resultados
}

// --- Implementações simuladas dos canais (substituíveis por Twilio, SES etc.) ---

// SMSNotificador simula o envio de SMS.
type SMSNotificador struct{}

func (SMSNotificador) Canal() string { return "sms" }

func (SMSNotificador) Enviar(ctx context.Context, destinatario, mensagem string) error {
	// Simula latência de rede respeitando cancelamento via context.Context.
	select {
	case <-time.After(120 * time.Millisecond):
		if destinatario == "" {
			return fmt.Errorf("telefone vazio")
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// EmailNotificador simula o envio de e-mail.
type EmailNotificador struct{}

func (EmailNotificador) Canal() string { return "email" }

func (EmailNotificador) Enviar(ctx context.Context, destinatario, mensagem string) error {
	select {
	case <-time.After(200 * time.Millisecond):
		if destinatario == "" {
			return fmt.Errorf("e-mail vazio")
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
