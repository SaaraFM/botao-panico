# Botão de Pânico — Microserviço da Plataforma da Mulher

Trabalho da disciplina **Tópicos Especiais em Tecnologias Digitais (CIT7219)**.

Microserviço em **Go** que implementa o botão de pânico da Plataforma da Mulher
(rede de apoio e segurança de mulheres em Araranguá/SC). Quando a usuária aciona
o botão, o serviço persiste o alerta no SQLite próprio e processa, através de mensageria assíncrona,
as notificações aos contatos de emergência e — em situações de risco alto ou crítico —
à rede institucional de proteção (Polícia Militar, Delegacia da Mulher, CRAS e SAMU).

O serviço expõe duas interfaces de comunicação simultâneas e integradas sobre a mesma camada de domínio:
1. **API REST (HTTP/JSON)** na porta `8080` (ou `8081` para integração local), consumida pela Supabase Edge Function da Plataforma da Mulher.
2. **API gRPC (HTTP/2 + Protocol Buffers binário)** na porta `50051`, voltada para comunicação de alta performance entre microserviços e integração institucional segura.
3. **Mensageria Assíncrona e Transactional Outbox (NATS JetStream)**: processamento desacoplado de notificações simuladas com persistência atômica, retentativas exponenciais e confirmação explícita (Ack).

---

## Como rodar

Requisito: Go 1.24 ou superior. Não precisa de compilador C, nem de Docker ou servidor de mensageria externo instalado (possui NATS JetStream e SQLite embutidos).

```bash
go run ./cmd/api
```

O serviço inicializa simultaneamente:
- **REST**: `http://localhost:8080`
- **gRPC**: `127.0.0.1:50051` (restrito a localhost por segurança)
- **NATS JetStream**: Servidor embutido restrito a `127.0.0.1:4222` com persistência em disco (`data/jetstream`).
- **Banco de dados**: SQLite próprio criado automaticamente com migração idempotente: `botao_panico.db`.

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
| `FILAS_ATIVAS` | `true` | Ativa mensageria NATS JetStream e Outbox assíncrono |
| `NATS_URL` | *(vazio)* | URL do NATS broker externo (vazio roda JetStream embutido) |
| `NATS_STREAM` | `ALERTAS` | Nome da Stream persistente do JetStream |
| `NATS_SUBJECT` | `alertas.notificar` | Subject de publicação e roteamento das tarefas |
| `NATS_CONSUMER` | `worker-notificacoes` | Nome do consumidor durável no JetStream |
| `NATS_STORE_DIR` | `data/jetstream` | Diretório de armazenamento de logs de mensagem no disco |
| `WORKER_SHUTDOWN_TIMEOUT` | `5s` | Prazo máximo de drenagem dos workers no encerramento |

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
| `CriarAlerta` | `CriarAlertaRequest` | `CriarAlertaResponse` | Registra alerta no SQLite com suporte a mensageria assíncrona e idempotência (`replay: true`). |
| `BuscarAlerta` | `BuscarAlertaRequest` | `BuscarAlertaResponse` | Consulta os dados de um alerta por ID. Retorna `NotFound` se inexistente. |
| `ListarAlertas` | `ListarAlertasRequest` | `ListarAlertasResponse` | **Segurança de Privacidade**: Exige `usuaria_id` no payload. Rejeita listagens globais indiscriminadas (`InvalidArgument`). |
| `ListarInstituicoes` | `ListarInstituicoesRequest` | `ListarInstituicoesResponse` | Retorna as instituições da rede de apoio de Araranguá/SC. |

### Como Executar o Cliente gRPC de Demonstração

```bash
# 1. Com o servidor rodando em outro terminal:
go run ./cmd/api

# 2. Em um novo terminal, execute o cliente de demonstração:
go run ./cmd/grpc-client
```

---

## 3. Mensageria Assíncrona e Filas com NATS JetStream (CIT7219 — Semana 12)

Para desacoplar a resposta imediata da requisição (`POST /alertas`) do processamento e despacho das notificações simuladas, o sistema implementa os padrões assíncronos fundamentais da Semana 12:

### Padrão Transactional Outbox
1. **Atomicidade no SQLite**: O registro do alerta na tabela `alertas` e a gravação do evento na tabela `tarefas_outbox` ocorrem rigorosamente na mesma transação SQLite (`BeginTx -> Commit`). Se o broker de mensagens estiver temporariamente fora do ar, o alerta nunca perde sua tarefa associada.
2. **Outbox Relay (Publisher)**: Uma goroutine em segundo plano monitora `tarefas_outbox` por gatilhos imediatos (`chan struct{}`) e varredura periódica configurável. Ao localizar tarefas pendentes, publica no NATS JetStream injetando o cabeçalho `Nats-Msg-Id` para deduplicação automática no broker.
3. **Stream Persistente**: A Stream `ALERTAS` armazena mensagens com persistência em disco (`Storage: FileStorage`), retenção `LimitsPolicy` e janela de deduplicação nativa de 5 minutos.

### Consumidor Durável e Pool de Workers
1. **Queue Subscription**: O consumidor (`ConsumidorNotificacoes`) assina a fila usando `QueueSubscribeSync` durável (`worker-notificacoes`), permitindo balanceamento automático de carga entre múltiplos workers concorrentes.
2. **Idempotência no Consumidor**: Antes de simular envios, o worker adquire trava por ID de alerta e verifica o status no repositório. Caso o alerta já tenha sido concluído (ex.: reentrega por timeout da rede), o worker confirma com `Ack()` sem reenviar notificações.
3. **Confirmação Explícita (Ack/Nak)**: O `Ack()` só é transmitido ao broker JetStream após a atualização atômica de `tarefas_outbox` e do alerta no SQLite. Se ocorrer erro transiente, envia `NakWithDelay()` com backoff exponencial.
4. **Desligamento Gracioso (Graceful Shutdown)**: O consumidor implementa `Drain()` e encerramento sincronizado via `sync.WaitGroup` e prazos de deadline (`WORKER_SHUTDOWN_TIMEOUT`).

---

## 4. Banco de dados (SQLite próprio)

Driver `modernc.org/sqlite` (Go puro). Tabelas criadas automaticamente com
`CREATE TABLE IF NOT EXISTS` (idempotente) em `internal/armazenamento/sqlite.go`:
- `alertas`: Registro histórico de acionamentos de pânico.
- `tarefas_outbox`: Tabela de mensageria assíncrona (Transactional Outbox).
- `notificacoes`: Log de notificações simuladas enviadas.
- `alerta_instituicoes`: Associação com a rede de apoio de Araranguá.
- `contatos_emergencia` e `instituicoes`: Cadastros de apoio e emergência.

---

## 5. Como rodar os testes

Todos os testes podem ser executados com:

```bash
# Análise estática de código
go vet ./...

# Testes unitários e de integração (REST + gRPC + Mensageria NATS + SQLite)
go test -v ./...

# Detecção rigorosa de condições de corrida em goroutines concorrentes
go test -race ./...

# Relatório de cobertura de código
go test -cover ./...
```

---

## 6. Estrutura em camadas

```
cmd/api/main.go           → inicialização REST, gRPC, NATS JetStream, Outbox Relay e Workers
cmd/grpc-client/main.go   → cliente gRPC de demonstração acadêmica
proto/v1/alerta.proto     → contrato Protocol Buffers versionado (alerta.v1)
gen/alerta/v1/            → stubs Go gerados pelo protoc (pb.go e grpc.pb.go)
internal/config/          → variáveis de ambiente (REST, gRPC, NATS) + validação estrita
internal/alerta/          → entidades, regras de negócio, Transactional Outbox e idempotência SHA-256
internal/fila/            → NATS JetStream (embutido/externo), Outbox Relay e Consumidor Durável
internal/grpcapi/         → adaptador gRPC, conversão de DTOs e interceptors S2S
internal/httpapi/         → adaptador REST HTTP/JSON e middleware S2S
internal/armazenamento/   → SQLite com idempotência e gravação atômica Alerta + Outbox
internal/cliente/         → client HTTP da API externa de geocodificação (Nominatim)
internal/notificacao/     → despacho simulado de SMS e E-mail com concorrência paralela
```

---

## 7. Onde cada conteúdo da disciplina aparece

| Conteúdo da disciplina                      | Onde está no código |
| ------------------------------------------- | ------------------- |
| Structs, arrays, slices e maps              | `internal/alerta/alerta.go`, `internal/alerta/outbox.go`, `internal/armazenamento/` |
| Ponteiros                                   | `*PedidoAlerta`, `*SQLite`, `*Servico`, `*GerenciadorNATS` (alertas trafegam por valor para evitar race condition) |
| Tratamento de erros e códigos gRPC          | `internal/alerta/erros.go`, `internal/grpcapi/conversores.go` (`codes.InvalidArgument`, `codes.AlreadyExists`, etc.) |
| Interfaces                                  | `Notificador`, `Repositorio`, `RepositorioOutbox`, `Geocodificador`, `alertav1.AlertasServiceServer` |
| Pacotes e arquitetura limpa em camadas      | `internal/...` (domínio isolado consumido por REST, gRPC e Mensageria) |
| Protocol Buffers e Contratos IDL            | `proto/v1/alerta.proto`, geração em `gen/alerta/v1/` |
| Comunicação RPC e gRPC                      | `internal/grpcapi/servidor.go`, `cmd/grpc-client/main.go` |
| Interceptors gRPC (Unary e Streaming)       | `internal/grpcapi/interceptors.go` (autenticação S2S via metadados) |
| Deadlines e Cancelamento distribuído        | `context.Context` com `context.WithDeadline` e `context.WithCancel` em REST, gRPC e Workers |
| Filas de Mensagens e Brokers Persistentes   | `internal/fila/nats.go`, Stream JetStream com retenção persistente e `Duplicates` window |
| Padrão Transactional Outbox                 | `internal/alerta/outbox.go`, `internal/armazenamento/outbox.go` (`SalvarAlertaComOutbox` em transação SQLite) |
| Outbox Relay e Publicação At-Least-Once     | `internal/fila/outbox_relay.go` com publicação confirmada (`Nats-Msg-Id`) e backoff exponencial |
| Consumidor Durável e Confirmação Explícita  | `internal/fila/consumidor.go` com `QueueSubscribeSync`, `Ack()`, `NakWithDelay()` e idempotência |
| Concorrência (goroutines e canais/select)   | `internal/notificacao/despachante.go`, loops do Relay e Workers no `cmd/api/main.go` |
| sync: WaitGroup, Mutex e sync.Map           | `ConsumidorNotificacoes.wg`, `OutboxRelay.wg`, `Servico.bloqueiosProcessamento`, `bloqueiosChave` |
| Testes com testing, testify e mocks         | `internal/fila/fila_test.go` (12 cenários de mensageria), `internal/grpcapi/servidor_test.go`, etc. |
