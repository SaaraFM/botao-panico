# Instruções para Agentes e Desenvolvedores — Botão de Pânico (Go)

Este repositório contém o microserviço do **Botão de Pânico em Go**, responsável pela persistência do alerta, Transactional Outbox e mensageria assíncrona via RabbitMQ para a rede de apoio.

---

## 1. Regra de Automação: Comando "sobe pro GitHub"

Sempre que a usuária enviar o comando exato **"sobe pro GitHub"**, o agente está expressamente **autorizado a executar o fluxo completo de publicação e merge**, seguindo rigorosamente as etapas abaixo:

1. **Inspeção de Estado**:
   - Identificar todas as alterações feitas e verificar `git status`, remoto e branch atual.
2. **Tratamento de Branch**:
   - Se o desenvolvimento estiver na `main`, criar imediatamente uma nova branch semântica (`feature/...` ou `fix/...`), movendo e preservando todo o trabalho existente.
3. **Validação e Segurança**:
   - Executar os testes automatizados relevantes (`go vet ./...` e `go test -count=1 ./...`).
   - Confirmar a ausência absoluta de segredos, tokens, senhas, `.env`, bancos SQLite (`.db`) ou arquivos da pasta `docs/`.
   - Se houver qualquer teste falhando, segredo detectado, conflito ou situação de risco: **PARAR IMEDIATAMENTE e consultar a usuária**.
4. **Commit Semântico**:
   - Preparar (`git add`) apenas os arquivos do projeto e realizar commit com mensagem descritiva (ex.: `feat(...)`, `fix(...)`, `test(...)`).
5. **Envio da Branch**:
   - Fazer push da nova branch para o repositório remoto oficial (`git push origin <nome-da-branch>`).
6. **Criação do Pull Request**:
   - Criar o Pull Request da branch para a `main` via GitHub CLI (`gh pr create`).
7. **Merge e Sincronização Autorizados**:
   - Com todas as verificações aprovadas, realizar o merge do Pull Request (`gh pr merge --merge` ou `gh pr merge --squash`).
   - Mudar para a `main` local e sincronizar com o remoto (`git checkout main` e `git pull origin main`).
8. **Relatório Objetivo**:
   - Informar apenas o resultado final conciso, o hash do commit e o link do Pull Request no GitHub.

---

## 2. Regras Permanentes de Segurança e Integridade

- **Proibições Estritas**:
  - Proibido o uso de `git push --force`, rebase destrutivo ou reset que altere commits já publicados.
  - Nunca recriar o repositório nem a branch `main`.
  - Nunca misturar arquivos ou históricos com o repositório da Plataforma da Mulher.
  - Não fazer deploy ou alterações de infraestrutura.
- **Proteção da pasta `docs/`**: A pasta `docs/` (guias de estudo e apresentação pessoal) deve permanecer **exclusivamente no computador local** e ignorada pelo `.gitignore`. Nenhum arquivo dessa pasta pode entrar no Git.
- **Segredos e Credenciais**:
  - Nunca versionar arquivos `.env`, chaves de serviço (`SERVICE_KEY`), senhas de SMTP ou tokens JWT.
  - Manter autenticação Serviço-a-Serviço (S2S) via `X-Service-Token` ou `Authorization: Bearer` validada em tempo constante.
- **Bancos de Dados e Logs**:
  - Nunca versionar arquivos `.db` ou persistências do SQLite.
  - Observabilidade estruturada com `log/slog` em formato JSON, sem expor coordenadas de GPS, dados pessoais ou credenciais nos logs.
- **Simulação de Notificações**:
  - Em ambiente de desenvolvimento e acadêmico, SMS e e-mails devem permanecer **estritamente simulados**.
  - `EMAIL_DEMO_ATIVO` deve permanecer `false` por padrão. Nunca disparar notificações reais para órgãos de emergência ou contatos de terceiros.

---

## 3. Padrões Técnicos e Portas

- **Porta REST**: `8081` (evita conflito com o frontend na 8080).
- **Porta gRPC**: `50051`.
- **Mensageria RabbitMQ**: porta AMQP `5672` e painel `15672`. Exchange: `panico.eventos`.
- **Testes Obrigatórios**:
  - `go vet ./...` (0 erros)
  - `go test -count=1 ./...` (100% de aprovação)
