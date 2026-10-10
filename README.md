# Botão de Pânico — Microserviço da Plataforma da Mulher

Trabalho da disciplina **Tópicos Especiais em Tecnologias Digitais (CIT7219)**.

Microserviço em **Go** que implementa o botão de pânico da Plataforma da Mulher
(rede de apoio e segurança de mulheres em Araranguá/SC). Quando a usuária aciona
o botão, o serviço notifica **em paralelo** seus contatos de emergência e — em
situações de risco alto ou crítico — também a rede institucional de proteção
(Polícia Militar, Delegacia da Mulher, CRAS e SAMU).

## Como rodar

Requisito: Go 1.24+. Não precisa de compilador C nem de servidor de banco.

```bash
cp .env.example .env        # opcional: ajuste os valores
export $(grep -v '^#' .env | xargs)   # carrega as variáveis (ou use os padrões)
go run ./cmd/api
```

O serviço sobe em `http://localhost:8080` e cria o arquivo `botao_panico.db`.

### Variáveis de ambiente

| Variável | Padrão | Descrição |
| --- | --- | --- |
| `PORT` | `8080` | Porta HTTP |
| `DATABASE_URL` | `file:botao_panico.db` | DSN do SQLite próprio do serviço |
| `EXTERNAL_API_URL` | `https://nominatim.openstreetmap.org` | API de geocodificação (vazio desativa) |
| `EXTERNAL_API_TIMEOUT` | `3s` | Timeout de cada chamada externa |
| `EXTERNAL_API_USER_AGENT` | identificação do trabalho | Exigido pela política do Nominatim |
| `NOTIFICATION_TIMEOUT` | `5s` | Prazo para concluir as notificações |

A configuração é validada na inicialização (porta, URL, durações).

### Acionando o botão de pânico

```bash
curl -X POST http://localhost:8080/alertas \
  -H "Content-Type: application/json" \
  -d '{"usuaria_id":"usuaria-demo","nivel":"alto","mensagem":"estou sendo seguida","local":{"latitude":-28.9346,"longitude":-49.4957}}'
```

### Endpoints

| Método | Rota | Sucesso | Erros possíveis |
| --- | --- | --- | --- |
| GET | `/health` | 200 | — |
| POST | `/alertas` | 201 | 400, 413, 422, 500, 503 |
| GET | `/alertas` | 200 | 500, 503 |
| GET | `/alertas/{id}` | 200 | 404, 500, 503 |
| GET | `/instituicoes` | 200 | 500, 503 |

Formato padrão de erro: `{"erro": "mensagem", "codigo": "CODIGO"}` (+ `campos` na validação).
Códigos: `JSON_INVALIDO`, `CORPO_MUITO_GRANDE`, `DADOS_INVALIDOS`, `NAO_ENCONTRADO`,
`BANCO_INDISPONIVEL`, `TEMPO_ESGOTADO`, `ERRO_INTERNO`. Mensagens de SQL/rede ficam só no log.

## Banco de dados (SQLite próprio)

Driver `modernc.org/sqlite` (Go puro). Tabelas criadas automaticamente com
`CREATE TABLE IF NOT EXISTS` (idempotente) em `internal/armazenamento/sqlite.go`:
`alertas`, `notificacoes`, `alerta_instituicoes`, `contatos_emergencia`, `instituicoes`.
Dados de exemplo inseridos com `INSERT OR IGNORE`. Gravações usam transações e consultas parametrizadas.
Os dados persistem após reiniciar (teste `TestSQLite_PersisteAposReabrir`).

## API externa (Nominatim / OpenStreetMap)

`internal/cliente/nominatim.go` faz geocodificação reversa: converte as coordenadas
do alerta em um **endereço aproximado** (`endereco_aproximado`), útil para quem recebe o alerta.
Usa `http.NewRequestWithContext`, timeout, fecha o `Body`, verifica o status e valida o JSON.
Somente latitude/longitude são enviadas (nenhum dado pessoal), com `User-Agent` identificado.

**Se a API falhar** (fora do ar, timeout, resposta inválida), o alerta é registrado e as
notificações são enviadas normalmente — só sem endereço — e o erro vai para o log.
As coordenadas são sempre a informação principal. A consulta roda em paralelo às notificações.
O Nominatim público aceita ~1 requisição/s; para uso real, aponte `EXTERNAL_API_URL` para instância própria.

> As notificações SMS/e-mail continuam **simuladas** (não há credenciais de operadora);
> a interface `Notificador` permite plugar um provedor real.

## Como rodar os testes

```bash
go vet ./...
go test ./...
go test -race ./...
go test -cover ./...
```

## Estrutura em camadas

```
cmd/api/main.go           → ponto de entrada, desligamento gracioso
internal/config/          → variáveis de ambiente + validação
internal/alerta/          → domínio, validação, regras de negócio e erros
internal/armazenamento/   → repositório SQLite (real) e em memória (testes)
internal/cliente/         → client HTTP da API externa (Nominatim)
internal/notificacao/     → despacho paralelo de notificações (goroutines)
internal/httpapi/         → API REST em JSON e mapeamento de erros
```

## Onde cada conteúdo da disciplina aparece

| Conteúdo da disciplina                      | Onde está no código |
| ------------------------------------------- | ------------------- |
| Structs, arrays, slices e maps              | `internal/alerta/alerta.go`, `internal/armazenamento/` |
| Ponteiros                                   | `*PedidoAlerta`, `*SQLite`, `*Servico` (alertas trafegam por valor para evitar condição de corrida) |
| Tratamento de erros                         | `internal/alerta/erros.go`, `internal/httpapi/erros.go`, `errors.Is/As` |
| Interfaces                                  | `Notificador`, `Repositorio`, `Geocodificador`, `ServicoAlertas` |
| Pacotes e estrutura em camadas              | `internal/...` (domínio, serviço, armazenamento, HTTP) |
| JSON e validação                            | `internal/httpapi/handlers.go`, `internal/alerta/validacao.go` |
| Concorrência (goroutines e canais/select)   | `internal/notificacao/notificador.go` |
| sync: WaitGroup e Mutex                     | `Despachante.Despachar`, `Repositorio` |
| context.Context                             | timeout de envio em `Servico.Acionar` e cancelamento nos notificadores |
| Testes com testing e testify                | `*_test.go` (unitários, table-driven, mocks e testes de API com httptest) |

## Decisões de design

- **Concorrência real:** cada notificação é enviada por uma goroutine. O teste
  `TestDespachar_EnviaParaTodosEmParalelo` prova o ganho: 10 envios de 100 ms
  terminam em ~100 ms, não em ~1 s.
- **Timeout com context.Context:** se uma operadora travar, o alerta não fica
  pendurado — o contexto expira em 5 segundos e o resultado é registrado.
- **Dados de exemplo:** o banco já nasce (INSERT OR IGNORE) com contatos da
  `usuaria-demo` e a rede de proteção de Araranguá, permitindo demonstrar o
  serviço logo na primeira execução.
