package alerta

import (
	"fmt"
	"strings"
)

// ErrosValidacao agrega todos os problemas encontrados no pedido,
// para que a API responda de uma só vez tudo o que precisa ser corrigido.
type ErrosValidacao struct {
	Campos map[string]string `json:"campos"`
}

func (e *ErrosValidacao) Error() string {
	partes := make([]string, 0, len(e.Campos))
	for campo, msg := range e.Campos {
		partes = append(partes, fmt.Sprintf("%s: %s", campo, msg))
	}
	return "pedido inválido: " + strings.Join(partes, "; ")
}

// niveisValidos é o conjunto fechado de níveis de risco aceitos.
var niveisValidos = map[NivelRisco]bool{
	RiscoBaixo:    true,
	RiscoModerado: true,
	RiscoAlto:     true,
	RiscoCritico:  true,
}

// Validar verifica as regras de negócio do pedido de alerta.
// Retorna nil quando está tudo certo ou *ErrosValidacao com todos os problemas.
func (p *PedidoAlerta) Validar() error {
	erros := &ErrosValidacao{Campos: map[string]string{}}

	if strings.TrimSpace(p.UsuariaID) == "" {
		erros.Campos["usuaria_id"] = "é obrigatório"
	}

	if !niveisValidos[p.Nivel] {
		erros.Campos["nivel"] = "deve ser baixo, moderado, alto ou critico"
	}

	if len(p.Mensagem) > 500 {
		erros.Campos["mensagem"] = "deve ter no máximo 500 caracteres"
	}

	// Coordenadas válidas no globo; (0,0) indica localização não capturada.
	if p.Local.Latitude < -90 || p.Local.Latitude > 90 {
		erros.Campos["local.latitude"] = "deve estar entre -90 e 90"
	}
	if p.Local.Longitude < -180 || p.Local.Longitude > 180 {
		erros.Campos["local.longitude"] = "deve estar entre -180 e 180"
	}
	if p.Local.Latitude == 0 && p.Local.Longitude == 0 {
		erros.Campos["local"] = "localização é obrigatória para acionar o botão de pânico"
	}

	if len(erros.Campos) > 0 {
		return erros
	}
	return nil
}

// ExigeInstituicoes indica se o nível de risco exige acionar a rede
// institucional de proteção além dos contatos pessoais.
func (p *PedidoAlerta) ExigeInstituicoes() bool {
	return p.Nivel == RiscoAlto || p.Nivel == RiscoCritico
}
