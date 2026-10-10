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
)

// Erros da API externa, para o chamador decidir o que fazer com errors.Is.
var (
	ErrServicoIndisponivel = errors.New("serviço externo indisponível")
	ErrTimeout             = errors.New("tempo esgotado ao consultar serviço externo")
	ErrRespostaInvalida    = errors.New("resposta inesperada do serviço externo")
	ErrRequisicaoRejeitada = errors.New("serviço externo rejeitou a requisição")
)

// tamanhoMaximoResposta evita ler respostas gigantes por engano/ataque.
const tamanhoMaximoResposta = 1 << 20 // 1 MiB

// Nominatim consulta a geocodificação reversa do OpenStreetMap
// (https://nominatim.org/release-docs/develop/api/Reverse/) para obter o
// endereço aproximado das coordenadas do alerta. Só latitude e longitude
// são enviadas — nenhum dado pessoal da usuária sai do serviço.
type Nominatim struct {
	urlBase   string
	agente    string
	clienteHT *http.Client
}

// NovoNominatim cria o client. timeout limita cada requisição; agente
// identifica a aplicação (exigência da política de uso do Nominatim público).
func NovoNominatim(urlBase, agente string, timeout time.Duration) *Nominatim {
	return &Nominatim{
		urlBase:   strings.TrimRight(urlBase, "/"),
		agente:    agente,
		clienteHT: &http.Client{Timeout: timeout},
	}
}

// respostaReversa é o subconjunto do JSON do Nominatim que usamos.
type respostaReversa struct {
	NomeExibicao string `json:"display_name"`
	Erro         string `json:"error"`
}

// EnderecoAproximado implementa alerta.Geocodificador.
func (n *Nominatim) EnderecoAproximado(ctx context.Context, local alerta.Localizacao) (string, error) {
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
