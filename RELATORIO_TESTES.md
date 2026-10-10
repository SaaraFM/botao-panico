# Relatório de testes e validação

Data da execução: 09/10/2026  
Projeto: `botao-panico`  
Branch: `main`

## A — Resumo executivo

O microsserviço foi compilado, formatado, analisado com `go vet`, testado
normalmente e com o detector de corridas. Também foram executados testes HTTP
reais contra uma instância local, teste de persistência após reinicialização,
testes controlados de indisponibilidade da API externa e uma consulta única ao
Nominatim público.

- Testes automatizados: aprovados.
- Testes HTTP manuais: 12 cenários aprovados.
- Testes de persistência SQLite: aprovado.
- Testes do client Nominatim: aprovados, incluindo falhas controladas.
- Integração real com Nominatim: HTTP 200.
- Cobertura total: 78,7%.
- Bloqueios: nenhum bloqueio funcional.
- Ressalvas: `cmd/api` não possui testes próprios; a cobertura desse pacote é
  0% nos testes unitários, embora o executável tenha sido exercitado por
  inicialização real.

**Veredito: APTO COM RESSALVAS.** Os critérios funcionais foram atendidos e
comprovados, mas recomenda-se adicionar testes automatizados para o ponto de
entrada `cmd/api` e para o ciclo de inicialização/desligamento.

## B — Ambiente

- Sistema operacional: Windows.
- Go: `go1.27.0 windows/amd64`.
- Git: `2.50.1.windows.1`.
- Banco dos testes HTTP: `file:test-http.db`.
- Porta dos testes HTTP: `18080`.
- API externa nos testes HTTP: serviço local indisponível
  (`http://127.0.0.1:1`), com timeout de 200 ms.
- Nenhuma credencial foi usada e nenhuma notificação real foi enviada.

## C — Testes automáticos

| Comando | Resultado |
| --- | --- |
| `gofmt -w .` | aprovado |
| `gofmt -l .` | nenhum arquivo listado |
| `go mod verify` | aprovado |
| `go build ./...` | aprovado |
| `go vet ./...` | aprovado |
| `go test -count=1 ./...` | aprovado |
| `go test -race -count=1 ./...` | aprovado |
| `go test -v ./internal/cliente/...` | aprovado |

Pacotes testados: `internal/alerta`, `internal/armazenamento`,
`internal/cliente`, `internal/config`, `internal/httpapi` e
`internal/notificacao`. O pacote `cmd/api` informa `[no test files]`.

### Cobertura

O primeiro comando de cobertura falhou porque o PowerShell separou
incorretamente o argumento não citado `-coverprofile=coverage.out`, fazendo o
Go interpretar `.out` como pacote. O comando foi corrigido com o argumento
entre aspas:

```powershell
go test '-coverprofile=coverage.out' ./...
go tool cover '-func=coverage.out'
go tool cover '-html=coverage.out' '-o=coverage.html'
```

| Pacote | Cobertura |
| --- | ---: |
| `cmd/api` | 0,0% |
| `internal/alerta` | 90,4% |
| `internal/armazenamento` | 76,6% |
| `internal/cliente` | 96,1% |
| `internal/config` | 97,3% |
| `internal/httpapi` | 98,6% |
| `internal/notificacao` | 78,0% |
| **Total** | **78,7%** |

As menores coberturas estão no ponto de entrada `cmd/api`, em alguns caminhos
de erro do armazenamento em memória e em implementações simuladas de
notificação. Não foi imposta meta artificial de cobertura.

`coverage.out`, `coverage.html` e `/coverage/` foram incluídos no `.gitignore`.

## D — Testes HTTP

Servidor iniciado com `go run ./cmd/api`, banco SQLite temporário e API externa
controladamente indisponível. O alerta usado foi sintético:

```json
{
  "usuaria_id": "usuaria-demo",
  "nivel": "alto",
  "mensagem": "teste sintetico",
  "local": {"latitude": -28.9346, "longitude": -49.4957},
  "silencioso": true
}
```

| # | Método e rota | Esperado | Real | Resultado |
| ---: | --- | ---: | ---: | --- |
| 1 | `GET /health` | 200 | 200 | aprovado |
| 2 | `GET /instituicoes` | 200 | 200 | aprovado |
| 3 | `POST /alertas` válido | 201 | 201 | aprovado |
| 4 | `GET /alertas` | 200 | 200 | aprovado; alerta apareceu |
| 5 | `GET /alertas/{id}` | 200 | 200 | aprovado; ID correspondeu |
| 6 | `GET /alertas/nao-existe` | 404 | 404 | aprovado |
| 7 | `POST /alertas` inválido | 422 | 422 | aprovado |
| 8 | JSON malformado | 400 | 400 | aprovado |
| 9 | Campo desconhecido | 400 | 400 | aprovado |
| 10 | Corpo acima de 64 KiB | 413 | 413 | aprovado |
| 11 | `PUT /alertas` | 405 | 405 | aprovado |
| 12 | Contrato de erro | JSON padronizado | conforme | aprovado |

Exemplos de respostas confirmadas:

```json
{"status":"ok"}
```

```json
{"erro":"alerta não encontrado","codigo":"NAO_ENCONTRADO"}
```

```json
{"erro":"corpo da requisição não é um JSON válido","codigo":"JSON_INVALIDO"}
```

As respostas de erro não expuseram mensagens de SQL, rede ou outros detalhes
internos.

## E — Persistência SQLite

1. A aplicação foi iniciada com `file:test-http.db`.
2. Foi criado o alerta `alerta-1664535ed38430b4`.
3. O registro foi consultado com HTTP 200.
4. O processo foi encerrado.
5. A aplicação foi reiniciada com o mesmo DSN.
6. O mesmo ID foi consultado novamente e retornou HTTP 200, com os mesmos
   campos principais (`usuaria_id`, `nivel`, `mensagem`, `local` e `status`).

Os testes automatizados de `internal/armazenamento` também cobrem criação do
schema, transações, relacionamentos, persistência após reabertura e registro
inexistente.

## F — API externa

O client Nominatim foi testado com `go test -v ./internal/cliente/...`.
Passaram os cenários HTTP 200, 400, 429, 500, 204 inesperado, JSON inválido,
`display_name` ausente, erro no corpo, tipo inesperado, timeout, contexto com
prazo e conexão recusada.

A implementação usa `http.NewRequestWithContext`, timeout no `http.Client`,
`defer resp.Body.Close()`, `User-Agent`, limite de 1 MiB para resposta,
verificação do status antes do JSON e classificação explícita dos erros.

Foi feita uma única consulta real ao Nominatim público, com coordenadas
públicas/sintéticas e User-Agent identificado. Resultado: HTTP 200 com resposta
JSON válida. Não foram feitos testes de carga.

## G — Segurança e concorrência

- `go test -race -count=1 ./...`: aprovado, sem data races.
- Notificações usam goroutines, `WaitGroup`, mutex e canais.
- Alertas são trafegados por valor e cópias profundas das slices são usadas.
- O armazenamento em memória protege o estado com mutex.
- SQLite usa consultas parametrizadas e transações.
- O corpo HTTP é limitado a 64 KiB.
- Campos JSON desconhecidos são rejeitados.
- Mensagens técnicas ficam no log e não são devolvidas ao cliente.
- Variáveis de ambiente com valores privados não são armazenadas no relatório.

No cenário de API externa indisponível, o alerta foi registrado e concluído com
HTTP 201; o fluxo continuou sem `endereco_aproximado`, conforme o contrato.

## H — Auditoria dos 14 critérios

| # | Critério | Status | Evidência |
| ---: | --- | --- | --- |
| 1 | Estrutura de pastas coerente | ATENDIDO | `cmd/` e camadas em `internal/` |
| 2 | Configuração por variáveis de ambiente | ATENDIDO | `internal/config/config.go`; casos válidos e inválidos testados |
| 3 | `go.mod` limpo | ATENDIDO | `go mod verify`, build e testes aprovados |
| 4 | Banco/schema próprio | ATENDIDO | SQLite, migração idempotente e sementes |
| 5 | Repository/store isolado | ATENDIDO | interfaces em `alerta` e implementações em `armazenamento` |
| 6 | Não encontrado separado de infraestrutura | ATENDIDO | HTTP 404 para ID ausente e códigos distintos para banco |
| 7 | Comunicação REST externa segura | ATENDIDO | contexto, timeout, status, JSON, limite e User-Agent |
| 8 | Rotas RESTful e códigos HTTP | ATENDIDO | 12 cenários HTTP reais aprovados |
| 9 | JSON de entrada e saída definido | ATENDIDO | structs, tags JSON e contratos exercitados |
| 10 | Erros de negócio separados de infraestrutura | ATENDIDO | `ErrosValidacao`, erros sentinel e tradutor HTTP |
| 11 | Tratamento idiomático de erros | ATENDIDO | `errors.Is/As`, `errors.Join`, propagação e logs |
| 12 | Interfaces onde necessário | ATENDIDO | repositório, geocodificador, notificadores e serviço HTTP |
| 13 | Nomenclatura e organização consistentes | ATENDIDO | `gofmt`, `go vet` e estrutura revisada |
| 14 | Testes unitários de negócio/handlers | ATENDIDO | pacotes de domínio e HTTP aprovados; ressalva no `cmd/api` |

## I — Alterações efetuadas

- Formatação Go aplicada com `gofmt`; nenhum arquivo permaneceu listado por
  `gofmt -l .`.
- Adicionado o caso de teste de `EXTERNAL_API_USER_AGENT` vazio em
  `internal/config/config_test.go`.
- Atualizado `.gitignore` para excluir `coverage.out`, `coverage.html` e
  `/coverage/`.
- Criados `coverage.out`, `coverage.html` e este relatório localmente; os
  relatórios de cobertura estão ignorados pelo Git.

O repositório já apresentava alterações não commitadas em diversos arquivos Go
antes desta validação. Elas foram preservadas; não foram executados `git
commit`, `git push`, reset ou descarte de alterações.

## J — Veredito

**APTO COM RESSALVAS.**

As verificações de compilação, qualidade, concorrência, HTTP, persistência,
configuração e integração externa passaram com evidências reais. A ressalva é
de cobertura estrutural: o pacote executável `cmd/api` não possui testes
próprios e aparece com 0,0% na cobertura unitária. Isso não impediu a
inicialização real nem os testes HTTP, mas testes automatizados do ciclo de
vida do executável seriam recomendáveis antes de uma versão de produção.
