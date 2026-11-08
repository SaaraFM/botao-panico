// Package cliente contém os clients HTTP de APIs externas consumidas pelo microserviço.
package cliente

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/plataforma-mulher/botao-panico/internal/alerta"
	"github.com/plataforma-mulher/botao-panico/internal/observabilidade"
	"github.com/plataforma-mulher/botao-panico/internal/resiliencia"
)

// Erros da API externa, para o chamador decidir o que fazer com errors.Is.
var (
	ErrServicoIndisponivel = errors.New("serviço externo indisponível")
	ErrTimeout             = errors.New("tempo esgotado ao consultar serviço externo")
	ErrRespostaInvalida    = errors.New("resposta inesperada do serviço externo")
	ErrRequisicaoRejeitada = errors.New("serviço externo rejeitou a requisição")
	// ErrCircuitoAberto é retornado quando o Circuit Breaker impede chamadas por instabilidade.
	ErrCircuitoAberto = resiliencia.ErrCircuitoAberto
)

// tamanhoMaximoResposta evita ler respostas gigantes por engano/ataque.
const tamanhoMaximoResposta = 1 << 20 // 1 MiB

// OpcoesNominatim reúne os parâmetros de configuração do client Nominatim,
// incluindo limites do Circuit Breaker e intervalo mínimo do rate limiter.
type OpcoesNominatim struct {
	URLBase            string
	Agente             string
	Timeout            time.Duration
	MaxFalhasCB        int
	TimeoutRecuperacao time.Duration
	IntervaloMinimo    time.Duration
	ClienteHTTP        *http.Client
	Metricas           *observabilidade.Metricas
}

// Nominatim consulta a geocodificação reversa do OpenStreetMap
// (https://nominatim.org/release-docs/develop/api/Reverse/) para obter o
// endereço aproximado das coordenadas do alerta. Só latitude e longitude
// são enviadas — nenhum dado pessoal da usuária sai do serviço.
//
// Protegido por Circuit Breaker (Closed, Open, Half-Open) e Rate Limiter (política OSM).
type Nominatim struct {
	urlBase        string
	agente         string
	clienteHT      *http.Client
	circuitBreaker *resiliencia.CircuitBreaker
	limitador      *resiliencia.LimitadorTaxa
}

// NovoNominatimComOpcoes instancia o client Nominatim com controle completo de resiliência.
func NovoNominatimComOpcoes(opts OpcoesNominatim) *Nominatim {
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	clHT := opts.ClienteHTTP
	if clHT == nil {
		clHT = &http.Client{Timeout: timeout}
	}
	maxFalhas := opts.MaxFalhasCB
	if maxFalhas <= 0 {
		maxFalhas = 3
	}
	recup := opts.TimeoutRecuperacao
	if recup <= 0 {
		recup = 10 * time.Second
	}

	var aoMudar func(nome string, ant, novo resiliencia.EstadoCircuito)
	var aoRejeitar func(nome string)
	if opts.Metricas != nil {
		opts.Metricas.AtualizarEstadoCircuitBreaker("nominatim-osm", "CLOSED")
		aoMudar = func(nome string, ant, novo resiliencia.EstadoCircuito) {
			opts.Metricas.RegistrarTransicaoCircuitBreaker(nome, string(ant), string(novo))
		}
		aoRejeitar = func(nome string) {
			opts.Metricas.RegistrarRejeicaoCircuitBreaker(nome)
		}
	}

	cb := resiliencia.NovoCircuitBreaker(resiliencia.OpcoesCircuitBreaker{
		AoMudarEstado:      aoMudar,
		AoRejeitar:         aoRejeitar,
		Nome:               "nominatim-osm",
		MaxFalhas:          maxFalhas,
		TimeoutRecuperacao: recup,
		Classificador: func(err error) bool {
			// Erros que caracterizam falha de infraestrutura do serviço externo:
			// - Indisponibilidade (5xx, 429, conexão recusada, falha de rede/DNS)
			// - Timeout (context deadline ou timeout de socket)
			// - Resposta inválida (JSON truncado / HTML de erro 502)
			//
			// NOTA: ErrRequisicaoRejeitada (400) é erro de parâmetros da usuária,
			// logo NÃO é falha do serviço e não deve abrir o circuito.
			if errors.Is(err, ErrServicoIndisponivel) ||
				errors.Is(err, ErrTimeout) ||
				errors.Is(err, ErrRespostaInvalida) {
				return true
			}
			return false
		},
	})

	lim := resiliencia.NovoLimitadorTaxa(opts.IntervaloMinimo)

	return &Nominatim{
		urlBase:        strings.TrimRight(opts.URLBase, "/"),
		agente:         opts.Agente,
		clienteHT:      clHT,
		circuitBreaker: cb,
		limitador:      lim,
	}
}

// NovoNominatim cria o client usando configurações padrão seguras.
// Mantido para compatibilidade total com o restante da aplicação.
func NovoNominatim(urlBase, agente string, timeout time.Duration) *Nominatim {
	return NovoNominatimComOpcoes(OpcoesNominatim{
		URLBase:            urlBase,
		Agente:             agente,
		Timeout:            timeout,
		MaxFalhasCB:        3,
		TimeoutRecuperacao: 10 * time.Second,
		IntervaloMinimo:    0, // 0 por padrão para não penalizar testes legados
	})
}

// CircuitBreaker devolve a instância interna para testes e métricas.
func (n *Nominatim) CircuitBreaker() *resiliencia.CircuitBreaker {
	return n.circuitBreaker
}

// Limitador devolve a instância do limitador de taxa para testes e métricas.
func (n *Nominatim) Limitador() *resiliencia.LimitadorTaxa {
	return n.limitador
}

// respostaReversa é o subconjunto do JSON do Nominatim que usamos.
type respostaReversa struct {
	NomeExibicao string `json:"display_name"`
	Erro         string `json:"error"`
}

// EnderecoAproximado implementa alerta.Geocodificador protegido por Circuit Breaker e Rate Limiting.
func (n *Nominatim) EnderecoAproximado(ctx context.Context, local alerta.Localizacao) (string, error) {
	var endereco string

	err := n.circuitBreaker.Executar(ctx, func() error {
		// 1. Respeita a taxa máxima permitida pela política da API (ex: 1 req/s)
		if err := n.limitador.Aguardar(ctx); err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				return fmt.Errorf("%w: %v", ErrTimeout, err)
			}
			return err
		}

		// 2. Executa a chamada HTTP protegida
		end, errReq := n.executarRequisicaoHTTP(ctx, local)
		if errReq != nil {
			return errReq
		}
		endereco = end
		return nil
	})

	if err != nil {
		return "", err
	}
	return endereco, nil
}

func (n *Nominatim) executarRequisicaoHTTP(ctx context.Context, local alerta.Localizacao) (string, error) {
	params := url.Values{}
	params.Set("format", "jsonv2")
	params.Set("lat", strconv.FormatFloat(local.Latitude, 'f', 6, 64))
	params.Set("lon", strconv.FormatFloat(local.Longitude, 'f', 6, 64))
	params.Set("zoom", "18")
	params.Set("accept-language", "pt-BR")

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, n.urlBase+"/reverse?"+params.Encode(), nil)
	if err != nil {
		return "", fmt.Errorf("montar requisição: %w", err)
	}
	req.Header.Set("User-Agent", n.agente)
	req.Header.Set("Accept", "application/json")

	resp, err := n.clienteHT.Do(req)
	if err != nil {
		return "", classificarFalhaRede(err)
	}
	defer resp.Body.Close()

	// Verifica o status ANTES de interpretar o corpo.
	switch {
	case resp.StatusCode >= 500:
		return "", fmt.Errorf("%w: status %d", ErrServicoIndisponivel, resp.StatusCode)
	case resp.StatusCode == http.StatusTooManyRequests:
		return "", fmt.Errorf("%w: limite de requisições (429)", ErrServicoIndisponivel)
	case resp.StatusCode >= 400:
		return "", fmt.Errorf("%w: status %d", ErrRequisicaoRejeitada, resp.StatusCode)
	case resp.StatusCode != http.StatusOK:
		return "", fmt.Errorf("%w: status %d", ErrRespostaInvalida, resp.StatusCode)
	}

	var corpo respostaReversa
	if err := json.NewDecoder(io.LimitReader(resp.Body, tamanhoMaximoResposta)).Decode(&corpo); err != nil {
		if errors.Is(err, context.DeadlineExceeded) || isTimeout(err) {
			return "", fmt.Errorf("%w: %v", ErrTimeout, err)
		}
		return "", fmt.Errorf("%w: JSON inválido: %v", ErrRespostaInvalida, err)
	}
	if corpo.Erro != "" {
		return "", fmt.Errorf("%w: %s", ErrRespostaInvalida, corpo.Erro)
	}
	endereco := strings.TrimSpace(corpo.NomeExibicao)
	if endereco == "" {
		return "", fmt.Errorf("%w: display_name ausente", ErrRespostaInvalida)
	}
	return endereco, nil
}

// classificarFalhaRede separa timeout de indisponibilidade (conexão recusada, DNS...).
func classificarFalhaRede(err error) error {
	if errors.Is(err, context.DeadlineExceeded) || isTimeout(err) {
		return fmt.Errorf("%w: %v", ErrTimeout, err)
	}
	return fmt.Errorf("%w: %v", ErrServicoIndisponivel, err)
}

func isTimeout(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}
