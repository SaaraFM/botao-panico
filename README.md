# Botão de Pânico — Microserviço da Plataforma da Mulher

Trabalho da disciplina **Tópicos Especiais em Tecnologias Digitais (CIT7219)**.

Microserviço em **Go** que implementa o botão de pânico da Plataforma da Mulher
(rede de apoio e segurança de mulheres em Araranguá/SC). Quando a usuária aciona
o botão, o serviço notifica **em paralelo** seus contatos de emergência e — em
situações de risco alto ou crítico — também a rede institucional de proteção
(Polícia Militar, Delegacia da Mulher, CRAS e SAMU).

O serviço expõe duas interfaces de comunicação simultâneas e integradas sobre a mesma camada de domínio:
1. **API REST (HTTP/JSON)** na porta `8080` (ou `8081` para integração local), consumida pela Supabase Edge Function da Plataforma da Mulher.
2. **API gRPC (HTTP/2 + Protocol Buffers binário)** na porta `50051`, voltada para comunicação de alta performance entre microserviços e integração institucional segura.

---

## Como rodar

Requisito: Go 1.24 ou superior. Não precisa de compilador C nem de servidor de banco externo.

```bash
go run ./cmd/api
```

O serviço sobe simultaneamente:
- **REST**: `http://localhost:8080`
- **gRPC**: `127.0.0.1:50051` (restrito a localhost por segurança)
- Banco de dados SQLite criado automaticamente: `botao_panico.db`.

### Variáveis de ambiente

| Variável | Padrão | Descrição |
| --- | --- | --- |
| `PORT` | `8080` | Porta HTTP REST (use `8081` para integração com Plataforma da Mulher) |
| `GRPC_PORT` | `50051` | Porta do servidor gRPC |
| `GRPC_HOST` | `127.0.0.1` | Host de escuta gRPC (padrão seguro restrito a localhost) |
| `SERVICE_KEY` | *(vazio)* | Chave secreta serviço-a-serviço (mínimo 16 caracteres) |
| `EXIGIR_AUTENTICACAO` | `false` | Se `true`, falha a inicialização caso `SERVICE_KEY` esteja ausente |
| `DATABASE_URL` | `file:botao_panico.db` | DSN do SQLite próprio do serviço |
| `EXTERNAL_API_URL` | `https://nominatim.openstreetmap.org` | API de geocodificação (vazio desativa) |
| `EXTERNAL_API_TIMEOUT` | `3s` | Timeout de cada chamada externa |
| `EXTERNAL_API_USER_AGENT` | identificação do trabalho | Exigido pela política do Nominatim |
| `NOTIFICATION_TIMEOUT` | `5s` | Prazo para concluir as notificações |

A configuração é validada estritamente na inicialização (portas, URLs, durações e conflito entre portas).

---

## 1. Comunicação via REST (HTTP/JSON)

### Acionando o botão de pânico via curl

```bash
curl -X POST http://localhost:8080/alertas \
  -H "Content-Type: application/json" \
  -H "X-Service-Token: sua-chave-secreta-se-configurada" \
  -d '{"usuaria_id":"usuaria-demo","nivel":"alto","mensagem":"estou sendo seguida","local":{"latitude":-28.9346,"longitude":-49.4957}}'
```

### Endpoints REST

| Método | Rota | Sucesso | Erros possíveis |
| --- | --- | --- | --- |
| GET | `/health` | 200 | — |
| POST | `/alertas` | 201 (ou 200 Replay) | 400, 401, 409, 413, 422, 500, 503 |
| GET | `/alertas` | 200 | 401, 500, 503 |
| GET | `/alertas/{id}` | 200 | 401, 404, 500, 503 |
| GET | `/instituicoes` | 200 | 500, 503 |

Formato padrão de erro: `{"erro": "mensagem", "codigo": "CODIGO"}` (+ `campos` na validação).
Códigos: `JSON_INVALIDO`, `CORPO_MUITO_GRANDE`, `DADOS_INVALIDOS`, `NAO_AUTORIZADO`, `CONFLITO_IDEMPOTENCIA`, `NAO_ENCONTRADO`, `BANCO_INDISPONIVEL`, `TEMPO_ESGOTADO`, `ERRO_INTERNO`.

---

## 2. Comunicação via gRPC e Protocol Buffers (CIT7219 — Semanas 09 e 11)

A API gRPC implementa o contrato definido em `proto/v1/alerta.proto` (pacote versionado `alerta.v1`), gerando serialização binária compacta com verificação estrita de schema.

### Versões das Ferramentas e Dependências
- **Compilador Protoc**: `libprotoc 36.2`
- **Plugin Go Protoc (`protoc-gen-go`)**: `v1.36.12`
- **Plugin Go gRPC (`protoc-gen-go-grpc`)**: `v1.6.2`
- **Módulo gRPC (`google.golang.org/grpc`)**: `v1.85.0`
- **Módulo Protobuf (`google.golang.org/protobuf`)**: `v1.36.12`

### Métodos gRPC Disponíveis

| Método RPC | Requisição | Resposta | Descrição |
| --- | --- | --- | --- |
| `CriarAlerta` | `CriarAlertaRequest` | `CriarAlertaResponse` | Registra alerta no SQLite com geocodificação e notificações paralelas. Suporta idempotência (`replay: true`). |
| `BuscarAlerta` | `BuscarAlertaRequest` | `BuscarAlertaResponse` | Consulta os dados de um alerta por ID. Retorna `NotFound` se inexistente. |
| `ListarAlertas` | `ListarAlertasRequest` | `ListarAlertasResponse` | **Segurança de Privacidade**: Exige `usuaria_id` no payload. Rejeita listagens globais indiscriminadas (`InvalidArgument`). |
| `ListarInstituicoes` | `ListarInstituicoesRequest` | `ListarInstituicoesResponse` | Retorna as instituições da rede de apoio de Araranguá/SC. |

> **Nota sobre Streaming (`MonitorarAlertas`)**: O método de streaming de alertas para salas de operação está documentado como evolução futura (Entrega posterior), mantendo os interceptors de streaming já testados e prontos.

### Autenticação em gRPC
O servidor instala os interceptors `AutenticarUnary` e `AutenticarStream` (`internal/grpcapi/interceptors.go`). As requisições devem enviar o token de serviço nos metadados gRPC:
- Chave de metadado: `x-service-token: <SERVICE_KEY>` ou `authorization: Bearer <SERVICE_KEY>`.
- Validação efetuada com comparação em tempo constante (`subtle.ConstantTimeCompare`).
- Retorna `codes.Unauthenticated` caso a credencial esteja ausente ou incorreta.

### Como Executar o Cliente gRPC de Demonstração

O projeto inclui um cliente de demonstração completo em Go:

```bash
# 1. Com o servidor rodando em outro terminal:
go run ./cmd/api

# 2. Em um novo terminal, execute o cliente de demonstração:
go run ./cmd/grpc-client
```

Se o servidor exigir autenticação:
```powershell
$env:SERVICE_KEY="sua-chave-secreta-12345"
go run ./cmd/grpc-client
```

O cliente demonstrará automaticamente:
1. `CriarAlerta` enviando payload binário Protocol Buffers.
2. `BuscarAlerta` com os dados persistidos no SQLite.
3. Retentativa com a mesma chave comprovando resposta idempotente (`replay: true`).
4. `ListarInstituicoes` consultando os órgãos de Araranguá.

---

## 3. Banco de dados (SQLite próprio)

Driver `modernc.org/sqlite` (Go puro). Tabelas criadas automaticamente com
`CREATE TABLE IF NOT EXISTS` (idempotente) em `internal/armazenamento/sqlite.go`:
`alertas`, `notificacoes`, `alerta_instituicoes`, `contatos_emergencia`, `instituicoes`.

Possui migração automática que adiciona as colunas `chave_idempotencia` e `hash_requisicao` com índice único caso o banco tenha sido criado em versões anteriores do projeto.

---

## 4. Como rodar os testes

Todos os testes podem ser executados com:

```bash
# Análise estática de código
go vet ./...

# Testes unitários e de integração (REST + gRPC + Domínio + SQLite)
go test -v ./...

# Detecção rigorosa de condições de corrida em goroutines concorrentes
go test -race ./...

# Relatório de cobertura de código
go test -cover ./...
```

---

## 5. Estrutura em camadas

```
cmd/api/main.go           → inicialização concorrente REST e gRPC, graceful shutdown
cmd/grpc-client/main.go   → cliente gRPC de demonstração acadêmica
proto/v1/alerta.proto     → contrato Protocol Buffers versionado (alerta.v1)
gen/alerta/v1/            → stubs Go gerados pelo protoc (pb.go e grpc.pb.go)
internal/config/          → variáveis de ambiente (REST, gRPC, timeouts) + validação
internal/alerta/          → domínio, validação, regras de negócio e idempotência SHA-256
internal/grpcapi/         → adaptador gRPC, conversão de DTOs e interceptors S2S
internal/httpapi/         → adaptador REST HTTP/JSON e middleware S2S
internal/armazenamento/   → repositório SQLite com idempotência persistente
internal/cliente/         → client HTTP da API externa de geocodificação (Nominatim)
internal/notificacao/     → despacho concorrente de notificações (goroutines + WaitGroup)
```

---

## 6. Onde cada conteúdo da disciplina aparece

| Conteúdo da disciplina                      | Onde está no código |
| ------------------------------------------- | ------------------- |
| Structs, arrays, slices e maps              | `internal/alerta/alerta.go`, `internal/armazenamento/` |
| Ponteiros                                   | `*PedidoAlerta`, `*SQLite`, `*Servico` (alertas trafegam por valor para evitar race condition) |
| Tratamento de erros e códigos gRPC          | `internal/alerta/erros.go`, `internal/grpcapi/conversores.go` (`codes.InvalidArgument`, `codes.AlreadyExists`, etc.) |
| Interfaces                                  | `Notificador`, `Repositorio`, `Geocodificador`, `ServicoAlertas`, `alertav1.AlertasServiceServer` |
| Pacotes e arquitetura limpa em camadas      | `internal/...` (domínio isolado consumido por adaptadores REST e gRPC) |
| Protocol Buffers e Contratos IDL            | `proto/v1/alerta.proto`, geração em `gen/alerta/v1/` |
| Comunicação RPC e gRPC                      | `internal/grpcapi/servidor.go`, `cmd/grpc-client/main.go` |
| Interceptors gRPC (Unary e Streaming)       | `internal/grpcapi/interceptors.go` (autenticação S2S via metadados) |
| Deadlines e Cancelamento distribuído        | `context.Context` com `context.WithDeadline` e `context.WithCancel` em REST e gRPC |
| Concorrência (goroutines e canais/select)   | `internal/notificacao/despachante.go`, orquestração no `cmd/api/main.go` |
| sync: WaitGroup, Mutex e sync.Map           | `Despachante.Despachar`, serialização de idempotência em `Servico.bloqueiosChave` |
| Testes com testing, testify e bufconn       | `internal/grpcapi/servidor_test.go` (listener em memória sem abrir porta física), `*_test.go` |
