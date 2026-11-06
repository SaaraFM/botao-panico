# Botão de Pânico — Microserviço em Go

Protótipo de microserviço em **Go** desenvolvido para o módulo de contingência da **Plataforma da Mulher** (TCC — Rede de apoio à mulher em Araranguá/SC), alinhado aos conteúdos práticos da disciplina **Tópicos Especiais em Tecnologias Digitais (CIT7219)**.

O serviço recebe acionamentos de socorro, persiste os dados localmente com garantia transacional (**Transactional Outbox**) no **SQLite** e publica eventos assíncronos no broker **RabbitMQ (AMQP 0-9-1)**, distribuindo tarefas no padrão **Publish/Subscribe com dois consumidores independentes** e tratamento de falhas em **Dead-Letter Queue (DLQ)**.

---

## Tecnologias

- **Linguagem Go (1.24+)**: desenvolvida com a biblioteca padrão (`net/http`, `log/slog`, `sync`, `context`).
- **Comunicação REST**: API HTTP/JSON para integração com frontends e Edge Functions.
- **gRPC e Protocol Buffers**: interface binária tipada definida em `proto/v1/alerta.proto`.
- **SQLite (`modernc.org/sqlite`)**: banco relacional em Go puro (sem dependência de CGO), com migrações automáticas.
- **RabbitMQ (`github.com/rabbitmq/amqp091-go`)**: broker AMQP com Publisher Confirms, Topic Exchange e DLQ.
- **Docker Compose**: orquestração local do RabbitMQ (`rabbitmq:3-management`).

---

## Como Funciona

1. **Recepção e Idempotência:** A requisição chega via `POST /alertas`. Quando a autenticação serviço-a-serviço está configurada (`EXIGIR_AUTENTICACAO=true`), o serviço valida o cabeçalho `X-Service-Token` (nos testes locais padrão, essa validação fica desativada). O serviço calcula o hash SHA-256 da carga para evitar disparos duplicados (`X-Idempotency-Key`).
2. **Transactional Outbox:** O alerta (status inicial `"processando"`) e a tarefa de mensageria são gravados na mesma transação atômica do SQLite, reduzindo significativamente o risco de perda de eventos caso o broker ou a rede oscilem.
3. **Resposta Imediata:** A API responde com código `201 Created` em poucos milissegundos, liberando a interface do usuário.
4. **Publicação Confiável:** O componente em segundo plano `OutboxRelay` lê as tarefas pendentes e as publica na exchange topic `panico.eventos` com confirmação de entrega (**Publisher Confirms**).
5. **Publish/Subscribe com Dois Consumidores:**
   - **Consumidor A (Notificações):** escuta `alerta.criado`, simula o despacho para contatos e instituições, atualiza o status para `"concluido"` no SQLite e emite **ACK manual**.
   - **Consumidor B (Auditoria):** escuta o tópico curinga `#`, registra logs estruturados e arquiva um registro independente de auditoria na tabela `auditoria_eventos`.
6. **Dead-Letter Queue (DLQ):** Mensagens corrompidas ou que excedam o limite de retentativas recebem **NACK sem requeue** e são encaminhadas automaticamente à fila `panico.alertas.dlq`.

---

## Como Executar Localmente

### Pré-requisitos
- Go 1.24 ou superior instalado.
- Docker Desktop em execução.

### 1. Iniciar o RabbitMQ
Na raiz do repositório, inicie o broker:
```bash
docker compose up -d
```
O painel administrativo estará acessível em `http://localhost:15672` (usuário: `guest`, senha: `guest`).

### 2. Iniciar a API em Go
```bash
go run ./cmd/api
```
A API iniciará na porta padrão `8080` (REST) e `50051` (gRPC).
*(Opcional: defina a variável `PORT=8081` caso a porta 8080 já esteja em uso por outro serviço local).*

### 3. Teste Rápido com Dados Fictícios
Em outro terminal, envie um acionamento simulado:

**Linux / macOS / Git Bash (curl):**
```bash
curl -i -X POST http://localhost:8080/alertas \
  -H "Content-Type: application/json" \
  -H "X-Idempotency-Key: teste-demo-001" \
  -d '{"usuaria_id":"demo-01","nivel":"alto","mensagem":"Teste academico","local":{"latitude":-28.9346,"longitude":-49.4957}}'
```

**Windows (PowerShell):**
```powershell
$headers = @{ "Content-Type" = "application/json"; "X-Idempotency-Key" = "teste-demo-001" }
$corpo = @{ usuaria_id = "demo-01"; nivel = "alto"; mensagem = "Teste academico"; local = @{ latitude = -28.9346; longitude = -49.4957 } } | ConvertTo-Json
Invoke-RestMethod -Uri "http://localhost:8080/alertas" -Method POST -Headers $headers -Body $corpo | ConvertTo-Json
```

Para demonstrar a comunicação binária via gRPC:
```bash
go run ./cmd/grpc-client
```

Para encerrar o RabbitMQ preservando os dados persistidos:
```bash
docker compose down
```

---

## Funcionalidades Principais

- **Idempotência Garantida:** Reenvios com a mesma chave devolvem o alerta original (`replay: true`) sem duplicar registros ou notificações.
- **Desacoplamento Assíncrono:** Notificações são processadas em segundo plano via workers com controle de fluxo (`QoS = 1`).
- **Resiliência a Falhas:** Mensagens pendentes no SQLite são automaticamente retransmitidas com backoff exponencial caso o RabbitMQ fique temporariamente inacessível.
- **Registro de Auditoria:** Consumidor independente mantém um registro separado de todos os eventos recebidos na tabela de auditoria.
- **Observabilidade Estruturada:** Logs padronizados em JSON emitidos via `log/slog` contendo métricas de duração, rota, status e identificadores de correlação, sem exposição de credenciais ou dados sensíveis.

---

## Limitações e Escopo

- **Finalidade Acadêmica:** Projeto experimental desenvolvido para fins didáticos e demonstração de contingência; **não substitui os canais oficiais de socorro de emergência (190 e 180)**.
- **Notificações Simuladas:** O despacho de mensagens (SMS/e-mail) é executado em modo simulado local por padrão; nenhum serviço público ou contato de emergência real é acionado.
- **Integração de Frontend:** O microserviço é independente; a integração ponta a ponta com a interface visual da Plataforma da Mulher permanece em evolução contínua.
