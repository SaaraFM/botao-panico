# Botão de Pânico — Microserviço da Plataforma da Mulher

Trabalho da disciplina **Tópicos Especiais em Tecnologias Digitais (CIT7219)**.

Microserviço em **Go** que implementa o botão de pânico da Plataforma da Mulher
(rede de apoio e segurança de mulheres em Araranguá/SC). Quando a usuária aciona
o botão, o serviço notifica **em paralelo** seus contatos de emergência e — em
situações de risco alto ou crítico — também a rede institucional de proteção
(Polícia Militar, Delegacia da Mulher, CRAS e SAMU).

## Como rodar

Requisito: Go 1.24.13 ou superior. Não precisa de compilador C nem de servidor de banco.

```bash
go run ./cmd/api
```

O serviço sobe em `http://localhost:8080` e cria o arquivo `botao_panico.db`.
O comando usa os valores padrão da tabela abaixo. Para personalizar a configuração,
defina as variáveis de ambiente no terminal antes de executar o serviço. O arquivo
`.env.example` serve como referência; o programa não carrega um arquivo `.env` automaticamente.

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

Para demonstrar a persistência com `requests/alertas.http`, inicie a API e execute
primeiro o POST 2. Copie o ID retornado e coloque-o em `@idAlertaPersistencia` no
início do arquivo. Use esse ID no GET 8 e execute a consulta antes de reiniciar a
API; depois reinicie o serviço e consulte o mesmo ID no GET 9. Ambos devem responder
HTTP 200 com os mesmos dados. A consulta dinâmica do GET 9 também pode usar o ID
retornado pelo POST 2, desde que o cliente de requisições ainda tenha essa resposta
disponível. No arquivo atual, o GET 8 está configurado com um ID inexistente e retorna
HTTP 404; substitua-o pelo ID do alerta para realizar a demonstração de persistência.

## API externa (Nominatim / OpenStreetMap)

`internal/cliente/nominatim.go` faz geocodificação reversa: converte as coordenadas
do alerta em um **endereço aproximado** (`endereco_aproximado`), útil para quem recebe o alerta.
Usa `http.NewRequestWithContext`, timeout, fecha o `Body`, verifica o status e valida o JSON.
Somente latitude/longitude são enviadas (nenhum dado pessoal), com `User-Agent` identificado.

**Se a API falhar** (fora do ar, timeout, resposta inválida), o alerta é registrado e as
notificações são enviadas normalmente — só sem endereço — e o erro vai para o log.
As coordenadas são sempre a informação principal. A consulta roda em paralelo às notificações,
mas o serviço aguarda o resultado até o timeout configurado antes de concluir o alerta.
O Nominatim público aceita ~1 requisição/s; para uso real, aponte `EXTERNAL_API_URL` para instância própria.

> `SMSNotificador` e `EmailNotificador` são **simulações locais**: não conectam a provedores
> e não entregam SMS ou e-mails reais. `sucesso: true` registra apenas que a simulação
> terminou sem erro; não confirma entrega. A interface `Notificador` permite uma integração
> futura com provedor, que não faz parte desta versão.

### Campo `silencioso`

O campo opcional `silencioso` é aceito e preservado no alerta como preferência para a
interface/aparelho ou possível funcionalidade futura. Atualmente ele não altera a seleção
de destinatários nem o despacho: as notificações simuladas seguem o mesmo fluxo,
independentemente do valor. A ausência do campo continua compatível com as requisições atuais.

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

## Decisões de implementação

As notificações são processadas em paralelo usando goroutines. Um teste com dez envios
simulados confirma que eles podem ser concluídos ao mesmo tempo. O serviço também usa
`context.Context` para limitar o tempo de espera pelas notificações; o prazo padrão é
de cinco segundos e pode ser alterado pela variável `NOTIFICATION_TIMEOUT`.

Na primeira execução, o SQLite cria as tabelas e inclui dados de exemplo para a
`usuaria-demo` e para as instituições de apoio de Araranguá. Esses dados permitem
experimentar os endpoints sem precisar cadastrá-los manualmente.
