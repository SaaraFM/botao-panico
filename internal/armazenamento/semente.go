// Package armazenamento implementa a persistência do microserviço:
// SQLite (implementação real, usada pela aplicação) e memória (usada nos testes).
package armazenamento

import "github.com/plataforma-mulher/botao-panico/internal/alerta"

// UsuariaDemo é a usuária fictícia usada na demonstração do trabalho.
const UsuariaDemo = "usuaria-demo"

// instituicoesExemplo é a rede de apoio de Araranguá/SC (dados de exemplo).
func instituicoesExemplo() []alerta.InstituicaoApoio {
	return []alerta.InstituicaoApoio{
		{ID: "inst-1", Nome: "Polícia Militar — Araranguá", Tipo: "policia", Telefone: "190",
			Local: alerta.Localizacao{Latitude: -28.9346, Longitude: -49.4957}},
		{ID: "inst-2", Nome: "Delegacia de Proteção à Mulher", Tipo: "delegacia_mulher", Telefone: "180",
			Local: alerta.Localizacao{Latitude: -28.9355, Longitude: -49.4860}},
		{ID: "inst-3", Nome: "CRAS Araranguá — Assistência Social", Tipo: "assistencia_social", Telefone: "483521-0000",
			Local: alerta.Localizacao{Latitude: -28.9410, Longitude: -49.4900}},
		{ID: "inst-4", Nome: "SAMU — Atendimento de Urgência", Tipo: "saude", Telefone: "192",
			Local: alerta.Localizacao{Latitude: -28.9380, Longitude: -49.5000}},
	}
}

// contatosExemplo são os contatos de emergência da usuária de demonstração.
func contatosExemplo() []alerta.ContatoEmergencia {
	return []alerta.ContatoEmergencia{
		{ID: "ctt-1", Nome: "Maria (mãe)", Telefone: "+5548991110001", Email: "maria@exemplo.com"},
		{ID: "ctt-2", Nome: "Ana (amiga)", Telefone: "+5548991110002", Email: "ana@exemplo.com"},
		{ID: "ctt-3", Nome: "Juliana (vizinha)", Telefone: "+5548991110003", Email: "juliana@exemplo.com"},
	}
}
