package alerta

import "errors"

// Erros de negócio e de infraestrutura compartilhados pelas camadas.
// Os repositórios embrulham a causa real com fmt.Errorf("%w: ...") e a camada
// HTTP decide o status com errors.Is, sem conhecer detalhes do banco.
var (
	// ErrNaoEncontrado: o recurso pedido não existe (erro de negócio → 404).
	ErrNaoEncontrado = errors.New("registro não encontrado")

	// ErrBancoIndisponivel: não foi possível falar com o banco (infra → 503).
	ErrBancoIndisponivel = errors.New("banco de dados indisponível")

	// ErrEscrita: o banco respondeu, mas a gravação falhou (infra → 500).
	ErrEscrita = errors.New("falha ao gravar no banco de dados")

	// ErrLeitura: o banco respondeu, mas a consulta falhou (infra → 500).
	ErrLeitura = errors.New("falha ao ler do banco de dados")
)
