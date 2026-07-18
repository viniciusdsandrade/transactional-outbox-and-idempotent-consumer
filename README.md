# Transactional Outbox and Idempotent Consumer Lab

## Estado atual: bootstrap da Fase 1

O repositório está preparado para desenvolvimento local, mas a implementação do
fluxo de negócio ainda não começou. O bootstrap atual entrega:

- um monorepo Gradle com Java 21 e três aplicações Spring Boot vazias;
- um banco lógico PostgreSQL para cada serviço, com baseline Flyway `V1`;
- um broker Kafka local em modo KRaft;
- health checks dos três serviços e uma composição Docker executável;
- testes mínimos que confirmam o carregamento da aplicação e a migração inicial.

Ainda não existem endpoints de negócio, tabelas de domínio, eventos, publishers,
consumidores, idempotência, retries ou DLQ. Esses itens pertencem às fases seguintes
do laboratório e não fazem parte deste bootstrap.

### Pré-requisitos

- Java 21;
- Docker Engine com Docker Compose;
- GNU Make (opcional; os mesmos comandos podem ser executados diretamente).

O wrapper `./gradlew` fixa a versão do Gradle usada pelo projeto. As credenciais
`lab`/`lab` e as portas abaixo são destinadas somente ao ambiente local.

### Comandos do bootstrap

```bash
make test                # testes unitários e de contexto
make build               # gera os três bootJar
make config              # valida o docker-compose.yml
make up                  # constrói e inicia PostgreSQL, Kafka e os serviços
make smoke               # consulta o Actuator dos três serviços
make logs                # acompanha os logs da composição
make down                # encerra a composição e preserva dados locais
make reset               # encerra e remove volumes locais
```

Se houver mais de um contexto Docker configurado, o contexto pode ser escolhido
por execução, por exemplo `DOCKER_CONTEXT=default make up`.

### Portas locais

| Componente | Porta | Uso |
| --- | ---: | --- |
| orders-service | 8081 | Actuator e futura API de pedidos |
| payments-service | 8082 | Actuator e futura API de pagamentos |
| billing-service | 8083 | Actuator e futura API de cobrança |
| Kafka | 9092 | Listener para o host |
| PostgreSQL | 5432 | Bancos `orders_db`, `payments_db` e `billing_db` |

## Enunciado

Construa uma PoC backend que demonstre, de forma prática e testável, como processar um fluxo distribuído de negócio sem depender de transações distribuídas, usando os padrões **Transactional Outbox** e **Idempotent Consumer**.

O sistema deve simular um fluxo de criação de pedido, autorização de pagamento, emissão de cobrança/fatura e registro de notificação. Cada etapa deve ser executada por um serviço independente, com seu próprio banco de dados lógico, comunicando-se por eventos assíncronos publicados em um broker.

A PoC deve provar que o sistema continua correto mesmo quando ocorrem falhas comuns em sistemas distribuídos, como duplicidade de mensagens, queda do consumidor após processar uma mensagem, indisponibilidade temporária do broker, repetição de requisições HTTP pelo cliente, erro transitório no processamento e mensagens inválidas.

O objetivo não é criar um e-commerce completo. O objetivo é demonstrar domínio técnico sobre **consistência eventual**, **idempotência**, **mensageria confiável**, **falhas parciais**, **reprocessamento seguro** e **limites transacionais** em uma arquitetura baseada em eventos.

> Este projeto assume deliberadamente que mensagens podem ser entregues mais de uma vez. O objetivo não é impedir duplicidade no broker, mas tornar cada efeito de negócio seguro diante de duplicidade, retry e falha parcial.

## Nome e descrição sugeridos

Nome sugerido do repositório:

```text
transactional-outbox-idempotency-lab
```

Descrição curta para o GitHub:

```text
A production-oriented lab demonstrating transactional outbox, idempotent consumers, event-driven consistency, retries, dead-letter queues and failure-safe message processing with Java/Kotlin, Spring Boot, PostgreSQL and Kafka.
```

## Problema a resolver

Em sistemas distribuídos, uma operação de negócio raramente termina dentro de uma única transação local.

Um fluxo comum é:

1. Um pedido é criado no serviço de pedidos.
2. Um evento precisa ser enviado para o serviço de pagamentos.
3. O serviço de pagamentos autoriza ou rejeita o pagamento.
4. O resultado precisa acionar faturamento, notificação ou conciliação posterior.

Uma implementação ingênua normalmente faz:

```text
salva pedido no banco
publica evento no broker
retorna sucesso
```

Esse desenho parece simples, mas possui uma falha crítica: o banco e o broker não participam da mesma transação. Se o pedido for salvo, mas a aplicação cair antes de publicar o evento, o pedido ficará persistido e nenhum outro serviço saberá que ele existe. Se o evento for publicado, mas o banco falhar logo depois, consumidores podem agir sobre um pedido inexistente. Se o consumidor processar a mensagem e cair antes de confirmar o consumo, a mesma mensagem pode ser entregue novamente e gerar efeitos colaterais duplicados.

A solução esperada é pragmática:

```text
gravar o dado de negócio e o evento de saída na mesma transação local
publicar o evento posteriormente por um publicador assíncrono
assumir entrega pelo menos uma vez
proteger os consumidores contra duplicidade
tornar os efeitos colaterais idempotentes
```

A hipótese central do projeto é:

> O sistema deve aceitar que mensagens podem ser publicadas ou consumidas mais de uma vez, e ainda assim preservar a correção do negócio.

## Objetivos da PoC

A PoC deve demonstrar as seguintes capacidades:

1. Persistir alteração de estado e evento de saída na mesma transação local.
2. Publicar eventos de forma assíncrona a partir de uma tabela de outbox.
3. Consumir eventos assumindo possibilidade real de duplicidade.
4. Tornar comandos HTTP e consumidores idempotentes.
5. Evitar efeitos colaterais duplicados em pagamentos, faturas e notificações.
6. Registrar mensagens processadas por consumidor.
7. Separar falhas transitórias de falhas definitivas.
8. Encaminhar mensagens inválidas para uma DLQ.
9. Permitir reprocessamento controlado.
10. Provar o comportamento por testes automatizados e cenários de falha reproduzíveis.

## Escopo funcional

O domínio da PoC será um fluxo simplificado de pedidos e faturamento.

Fluxo principal:

```text
Order API recebe um pedido
Order Service salva o pedido e grava OrderCreated na outbox
Outbox Publisher publica OrderCreated no broker
Payment Service consome OrderCreated
Payment Service cria uma autorização de pagamento
Payment Service grava PaymentAuthorized ou PaymentRejected na outbox
Payment Outbox Publisher publica o resultado do pagamento
Billing Service consome PaymentAuthorized
Billing Service gera uma fatura
Billing Service grava InvoiceGenerated na outbox
Notification Service consome InvoiceGenerated
Notification Service registra uma notificação enviada
```

A versão ideal para portfólio usa quatro serviços:

```text
orders-service
payments-service
billing-service
notifications-service
```

Uma versão reduzida pode usar três serviços:

```text
orders-service
payments-service
billing-service
```

Nesse caso, a notificação pode ser representada por uma tabela dentro do `billing-service` ou por um consumidor secundário simples.

## Stack recomendada

Stack base:

```text
Java 21 ou Kotlin
Spring Boot
Spring Web
Spring Data JPA
PostgreSQL
Flyway
Kafka
Docker Compose
Testcontainers
JUnit 5
REST Assured
Awaitility
```

Ferramentas opcionais:

```text
ArchUnit
JaCoCo
Pitest
OpenTelemetry
Prometheus
Grafana
```

Kafka é recomendado porque comunica bem o tipo de desafio esperado em vagas backend pleno. Ainda assim, o objetivo principal não é Kafka em si, mas provar semântica de processamento, idempotência e confiabilidade.

## Não objetivos

Esta PoC não deve tentar resolver todos os problemas de arquitetura distribuída ao mesmo tempo.

Ficam fora do escopo obrigatório:

```text
frontend
login de usuário
painel administrativo
orquestração complexa de saga
event sourcing completo
CQRS completo
microservices com dezenas de endpoints
Kubernetes
Terraform
serviço real de pagamento
integração real com e-mail ou WhatsApp
uso obrigatório de Debezium
```

Também não se deve prometer "exactly once" de forma genérica. A formulação correta para esta PoC é:

```text
entrega pelo menos uma vez
processamento idempotente
efeitos de negócio não duplicados
consistência eventual
```

Duplicidade de mensagem deve ser tratada como uma condição esperada, não como um bug raro.

## Arquitetura conceitual

```text
HTTP Client
    |
    v
orders-service
    |
    | grava Order + OutboxEvent na mesma transação
    v
orders database
    |
    | Outbox Publisher lê eventos pendentes
    v
Kafka topic: orders.events
    |
    v
payments-service
    |
    | registra mensagem processada
    | cria Payment
    | grava PaymentAuthorized ou PaymentRejected na outbox
    v
payments database
    |
    | Outbox Publisher lê eventos pendentes
    v
Kafka topic: payments.events
    |
    v
billing-service
    |
    | registra mensagem processada
    | cria Invoice
    | grava InvoiceGenerated na outbox
    v
billing database
    |
    | Outbox Publisher lê eventos pendentes
    v
Kafka topic: billing.events
    |
    v
notifications-service
```

Cada serviço que publica eventos deve possuir sua própria tabela de outbox.

Cada serviço que consome eventos deve possuir sua própria tabela de mensagens processadas.

## Serviços obrigatórios

### Orders Service

Responsável por receber pedidos e iniciar o fluxo.

Endpoint mínimo:

```text
POST /orders
```

Payload sugerido:

```json
{
  "requestId": "client-generated-idempotency-key",
  "customerId": "customer-123",
  "items": [
    {
      "sku": "SKU-001",
      "quantity": 2,
      "unitPrice": 1000
    }
  ]
}
```

O campo `requestId` deve funcionar como chave de idempotência do comando HTTP. Se o cliente enviar duas requisições com o mesmo `requestId`, o sistema não deve criar dois pedidos.

Responsabilidades:

```text
validar o comando de criação de pedido
calcular valor total do pedido
criar registro em orders
gravar evento OrderCreated na outbox dentro da mesma transação
retornar o pedido criado
impedir criação duplicada por requestId
expor endpoint de consulta de status do pedido
```

Endpoints sugeridos:

```text
POST /orders
GET /orders/{orderId}
GET /orders/{orderId}/timeline
```

### Payments Service

Responsável por consumir `OrderCreated` e simular uma autorização de pagamento.

Responsabilidades:

```text
consumir eventos OrderCreated
registrar a mensagem recebida em processed_messages
garantir que o mesmo evento não gere dois pagamentos
criar uma autorização de pagamento
simular aprovação ou rejeição
gravar PaymentAuthorized ou PaymentRejected na outbox
publicar o resultado do pagamento pelo outbox publisher
```

O pagamento deve ser idempotente em dois níveis:

```text
nível de mensagem: o mesmo eventId não pode ser processado duas vezes pelo mesmo consumidor
nível de negócio: o mesmo orderId não pode gerar duas autorizações de pagamento bem-sucedidas
```

Essa distinção é importante. Registrar apenas `eventId` em `processed_messages` evita reprocessar a mesma mensagem, mas não protege necessariamente contra dois eventos diferentes representando a mesma intenção de negócio. Por isso, o serviço também deve ter uma restrição de unicidade por `orderId` ou por uma chave de negócio equivalente.

Endpoints auxiliares:

```text
GET /payments/{paymentId}
GET /payments/by-order/{orderId}
POST /internal/payment-provider/mode
```

O endpoint de modo do provedor pode permitir simular:

```text
sempre aprovar
sempre rejeitar
falhar temporariamente
demorar para responder
retornar resposta inválida
```

### Billing Service

Responsável por consumir `PaymentAuthorized` e gerar uma fatura.

Responsabilidades:

```text
consumir eventos PaymentAuthorized
registrar a mensagem recebida em processed_messages
impedir geração duplicada de fatura para o mesmo orderId
criar invoice
gravar InvoiceGenerated na outbox
publicar InvoiceGenerated pelo outbox publisher
ignorar PaymentRejected para faturamento
```

Regras mínimas:

```text
não pode existir fatura para pagamento rejeitado
não pode existir mais de uma fatura para o mesmo pedido
uma mensagem duplicada de PaymentAuthorized não pode gerar fatura duplicada
uma fatura já emitida não deve ser alterada destrutivamente
```

Endpoints sugeridos:

```text
GET /invoices/{invoiceId}
GET /invoices/by-order/{orderId}
```

### Notifications Service

Responsável por consumir `InvoiceGenerated` e registrar uma notificação enviada.

Responsabilidades:

```text
consumir InvoiceGenerated
registrar a mensagem em processed_messages
criar registro de notificação
impedir notificação duplicada para a mesma invoice
simular envio de e-mail sem enviar e-mail real
```

Endpoint sugerido:

```text
GET /notifications/by-invoice/{invoiceId}
```

A notificação pode ser apenas um registro em banco:

```text
invoiceId
recipient
template
status
sentAt
```

## Modelo de dados mínimo

### `orders`

Campos sugeridos:

```text
id
request_id
customer_id
status
total_amount
created_at
updated_at
```

Restrições:

```text
request_id único
id único
```

Status sugeridos:

```text
CREATED
PAYMENT_PENDING
PAYMENT_AUTHORIZED
PAYMENT_REJECTED
INVOICED
```

### `payments`

Campos sugeridos:

```text
id
order_id
status
amount
provider_transaction_id
created_at
updated_at
```

Restrições:

```text
order_id único
provider_transaction_id único quando existir
```

Status sugeridos:

```text
AUTHORIZED
REJECTED
FAILED
```

### `invoices`

Campos sugeridos:

```text
id
order_id
payment_id
invoice_number
amount
status
issued_at
created_at
```

Restrições:

```text
order_id único
payment_id único
invoice_number único
```

Status sugeridos:

```text
ISSUED
CANCELLED
```

Cancelamento pode ficar fora do escopo desta PoC.

### `notifications`

Campos sugeridos:

```text
id
invoice_id
recipient
template_name
status
sent_at
created_at
```

Restrição:

```text
invoice_id + template_name únicos
```

Status sugeridos:

```text
SENT
FAILED
```

### `outbox_events`

Cada serviço publicador deve ter sua própria tabela `outbox_events`.

Campos sugeridos:

```text
id
aggregate_type
aggregate_id
event_type
event_version
payload
headers
status
attempt_count
next_attempt_at
created_at
published_at
last_error
locked_at
locked_by
```

Status sugeridos:

```text
PENDING
PROCESSING
PUBLISHED
FAILED
DEAD
```

Mesmo depois de publicado, o evento pode permanecer na tabela para auditoria e diagnóstico.

### `processed_messages`

Cada serviço consumidor deve ter sua própria tabela `processed_messages`.

Campos sugeridos:

```text
id
message_id
consumer_name
event_type
aggregate_id
processed_at
```

Restrição:

```text
message_id + consumer_name únicos
```

Essa tabela é a primeira barreira contra duplicidade de mensagens. A segunda barreira deve ser composta por restrições de negócio, como `payments.order_id`, `invoices.order_id` e `notifications.invoice_id + template_name`.

## Contrato dos eventos

Todos os eventos devem possuir metadados padronizados.

Envelope sugerido:

```json
{
  "eventId": "uuid",
  "eventType": "OrderCreated",
  "eventVersion": 1,
  "aggregateType": "Order",
  "aggregateId": "order-uuid",
  "correlationId": "uuid",
  "causationId": "uuid-or-null",
  "idempotencyKey": "business-key",
  "occurredAt": "timestamp",
  "producer": "orders-service",
  "payload": {}
}
```

Campos obrigatórios:

```text
eventId
eventType
eventVersion
aggregateType
aggregateId
correlationId
occurredAt
producer
payload
```

Campos recomendados:

```text
causationId
idempotencyKey
```

Finalidade dos campos:

```text
eventId: identifica a mensagem específica
eventType: identifica o tipo do evento
eventVersion: permite evolução de contrato
aggregateType: identifica o tipo do agregado de negócio
aggregateId: identifica o agregado de negócio
correlationId: conecta todos os eventos do mesmo fluxo
causationId: aponta qual evento causou o evento atual
idempotencyKey: permite deduplicação por intenção de negócio
occurredAt: registra quando o evento aconteceu
producer: identifica qual serviço produziu o evento
payload: contém os dados específicos do evento
```

## Eventos obrigatórios

### `OrderCreated`

Produzido por `orders-service` e consumido por `payments-service`.

Payload sugerido:

```json
{
  "orderId": "order-uuid",
  "customerId": "customer-123",
  "totalAmount": 2000,
  "currency": "BRL",
  "items": [
    {
      "sku": "SKU-001",
      "quantity": 2,
      "unitPrice": 1000
    }
  ]
}
```

### `PaymentAuthorized`

Produzido por `payments-service` e consumido por `billing-service`.

Payload sugerido:

```json
{
  "paymentId": "payment-uuid",
  "orderId": "order-uuid",
  "amount": 2000,
  "currency": "BRL",
  "providerTransactionId": "provider-transaction-id",
  "authorizedAt": "timestamp"
}
```

### `PaymentRejected`

Produzido por `payments-service` e consumido opcionalmente por `orders-service`.

Payload sugerido:

```json
{
  "paymentId": "payment-uuid",
  "orderId": "order-uuid",
  "amount": 2000,
  "currency": "BRL",
  "reason": "INSUFFICIENT_FUNDS",
  "rejectedAt": "timestamp"
}
```

Para simplificar, o `billing-service` não precisa consumir esse evento. O `orders-service` pode consumi-lo para atualizar o status do pedido.

### `InvoiceGenerated`

Produzido por `billing-service` e consumido por `notifications-service`.

Payload sugerido:

```json
{
  "invoiceId": "invoice-uuid",
  "orderId": "order-uuid",
  "paymentId": "payment-uuid",
  "invoiceNumber": "INV-2026-000001",
  "amount": 2000,
  "currency": "BRL",
  "issuedAt": "timestamp"
}
```

## Fluxo feliz obrigatório

O fluxo feliz deve ser demonstrável a partir de uma requisição:

```text
POST /orders
```

Resultado esperado:

```text
pedido criado
evento OrderCreated gravado na outbox do orders-service
evento OrderCreated publicado no broker
payments-service consome OrderCreated
pagamento autorizado
evento PaymentAuthorized gravado na outbox do payments-service
evento PaymentAuthorized publicado no broker
billing-service consome PaymentAuthorized
fatura emitida
evento InvoiceGenerated gravado na outbox do billing-service
notifications-service consome InvoiceGenerated
notificação registrada
```

O README deve mostrar como verificar o resultado final usando endpoints HTTP ou consultas ao banco:

```text
GET /orders/{orderId}
GET /payments/by-order/{orderId}
GET /invoices/by-order/{orderId}
GET /notifications/by-invoice/{invoiceId}
```

## Cenários de falha obrigatórios

### Duplicidade de requisição HTTP

Cenário:

```text
o cliente envia duas vezes POST /orders com o mesmo requestId
```

Resultado esperado:

```text
apenas um pedido é criado
a segunda requisição retorna o pedido já existente ou uma resposta idempotente equivalente
apenas um evento OrderCreated é gravado
nenhum pagamento duplicado é gerado
```

Critério de aceite:

```text
a tabela orders contém apenas um registro para o requestId
a tabela outbox_events contém apenas um OrderCreated para aquele pedido
```

### Queda após persistir o pedido, antes de publicar o evento

Cenário:

```text
orders-service cria o pedido e grava OrderCreated na outbox
a aplicação cai antes de publicar no broker
```

Resultado esperado:

```text
o evento permanece como PENDING na outbox
quando o serviço volta, o outbox publisher publica o evento
o fluxo continua normalmente
```

Critério de aceite:

```text
nenhum pedido fica invisível para os demais serviços
eventos pendentes são eventualmente publicados
```

### Broker indisponível temporariamente

Cenário:

```text
pedido é criado
evento é salvo na outbox
Kafka fica indisponível por alguns segundos ou minutos
```

Resultado esperado:

```text
o evento não é perdido
o evento permanece pendente ou com tentativa falha registrada
o outbox publisher tenta novamente depois
quando o broker volta, o evento é publicado
```

Critério de aceite:

```text
attempt_count aumenta
last_error é preenchido quando houver falha
status não é marcado como PUBLISHED sem confirmação de publicação
```

### Publicação duplicada do mesmo evento

Cenário:

```text
outbox publisher publica o evento no broker
mas falha antes de marcar o registro como PUBLISHED
na próxima execução, publica o mesmo evento novamente
```

Resultado esperado:

```text
consumidores recebem evento duplicado
processed_messages impede reprocessamento da mesma mensagem
restrições de negócio impedem efeitos colaterais duplicados
```

Critério de aceite:

```text
apenas um pagamento é criado para o pedido
apenas uma fatura é criada para o pedido
apenas uma notificação é criada para a fatura
```

### Consumidor cai depois de processar, mas antes de confirmar consumo

Cenário:

```text
payments-service consome OrderCreated
cria pagamento
grava PaymentAuthorized na outbox
commita a transação local
cai antes de confirmar o offset/ack no broker
```

Resultado esperado:

```text
broker entrega a mesma mensagem novamente
payments-service detecta que a mensagem já foi processada
nenhum segundo pagamento é criado
nenhum segundo PaymentAuthorized de negócio é gerado
```

Critério de aceite:

```text
processed_messages contém o eventId processado
payments contém apenas um pagamento para orderId
```

### Mensagem inválida

Cenário:

```text
um evento com payload inválido chega ao consumidor
```

Exemplos de invalidade:

```text
campo obrigatório ausente
eventVersion não suportada
tipo de evento desconhecido
payload incompatível com o contrato esperado
```

Resultado esperado:

```text
a mensagem não deve gerar efeito de negócio
a falha deve ser registrada
a mensagem deve ir para uma DLQ ou tabela equivalente de mensagens mortas
o consumidor não deve ficar em loop infinito processando a mesma mensagem inválida
```

Critério de aceite:

```text
nenhum pagamento ou fatura é criado a partir de mensagem inválida
a mensagem inválida fica rastreável para diagnóstico
```

### Pagamento rejeitado

Cenário:

```text
pedido é criado
payments-service simula rejeição de pagamento
```

Resultado esperado:

```text
PaymentRejected é gerado
nenhuma fatura é emitida
pedido pode ser marcado como PAYMENT_REJECTED
```

Critério de aceite:

```text
não existe invoice para orderId rejeitado
existe registro de pagamento rejeitado
existe evento PaymentRejected publicado
```

### Eventos fora de ordem

Cenário:

```text
billing-service recebe PaymentAuthorized antes de possuir qualquer conhecimento local do pedido
```

Resultado esperado:

```text
billing-service deve conseguir emitir a fatura usando os dados presentes no próprio evento
ou deve registrar pendência de processamento, se decidir depender de estado local
```

Para esta PoC, a abordagem recomendada é que `PaymentAuthorized` carregue os dados mínimos necessários para emissão da fatura. Isso reduz acoplamento entre serviços.

Critério de aceite:

```text
billing-service não deve chamar orders-service de forma síncrona para conseguir processar PaymentAuthorized
```

## Regras de idempotência

A PoC deve implementar idempotência em três camadas.

### Idempotência de comando HTTP

Aplicável a:

```text
POST /orders
```

Mecanismo:

```text
requestId único informado pelo cliente
constraint única no banco
retorno consistente em caso de repetição
```

Objetivo:

```text
evitar criação duplicada de pedidos quando o cliente faz retry
```

### Idempotência de mensagem

Aplicável a consumidores Kafka.

Mecanismo:

```text
processed_messages com message_id + consumer_name únicos
```

Objetivo:

```text
evitar reprocessamento da mesma mensagem pelo mesmo consumidor
```

### Idempotência de negócio

Aplicável a:

```text
pagamentos
faturas
notificações
```

Mecanismo:

```text
constraints únicas por chave de negócio
```

Exemplos:

```text
payments.order_id único
invoices.order_id único
notifications.invoice_id + template_name únicos
```

Objetivo:

```text
evitar efeitos colaterais duplicados mesmo quando mensagens diferentes representam a mesma intenção de negócio
```

Esta é a camada mais importante do ponto de vista de negócio.

## Publicador de outbox

Cada serviço que produz eventos deve ter um componente responsável por publicar eventos pendentes.

Responsabilidades:

```text
buscar eventos PENDING
bloquear lote de eventos para evitar concorrência entre publicadores
publicar evento no broker
marcar evento como PUBLISHED somente após confirmação de publicação
incrementar attempt_count em caso de falha
registrar last_error
respeitar next_attempt_at para backoff simples
marcar como DEAD após excesso de tentativas, se aplicável
```

Comportamento esperado:

```text
se a publicação falhar, o evento continua disponível para nova tentativa
se a publicação for bem-sucedida, mas a marcação como PUBLISHED falhar, o evento pode ser publicado novamente
consumidores devem tolerar duplicidade
```

O projeto deve explicar explicitamente esse trade-off.

## Consumidor idempotente

Cada consumidor deve seguir o fluxo abaixo:

```text
receber mensagem
abrir transação local
tentar inserir eventId + consumerName em processed_messages
se já existir, ignorar mensagem e confirmar consumo
validar contrato do evento
executar regra de negócio
gravar evento de saída na outbox, se houver
commitar transação
confirmar consumo no broker
```

A ordem importa.

A confirmação do consumo deve ocorrer somente depois da transação local ser concluída. Se o serviço cair após o commit e antes da confirmação, a mensagem poderá ser entregue novamente. Nesse caso, `processed_messages` deve impedir novo efeito de negócio.

## Tópicos Kafka sugeridos

```text
orders.events
payments.events
billing.events
orders.events.dlq
payments.events.dlq
billing.events.dlq
```

Também é possível usar um único tópico por domínio, mas nomes explícitos como `orders.events` deixam a PoC mais clara.

## Tratamento de erros

A PoC deve distinguir três tipos de erro.

### Erro transitório

Exemplos:

```text
banco temporariamente indisponível
broker temporariamente indisponível
timeout simulado no provedor de pagamento
```

Tratamento esperado:

```text
retry
backoff
manutenção do evento na outbox
não registrar como processado se o efeito de negócio não foi concluído
```

### Erro de contrato

Exemplos:

```text
payload inválido
eventVersion incompatível
eventType desconhecido
campo obrigatório ausente
```

Tratamento esperado:

```text
não executar regra de negócio
registrar erro
enviar para DLQ
não ficar tentando indefinidamente
```

### Erro de negócio esperado

Exemplos:

```text
pagamento rejeitado
pedido inválido
valor inconsistente
```

Tratamento esperado:

```text
gerar evento de rejeição ou registrar status final
não tratar como erro técnico
não enviar para DLQ se for uma decisão válida de negócio
```

## Critérios de aceite

### Critérios funcionais

```text
criar pedido por HTTP
publicar OrderCreated via outbox
consumir OrderCreated no payments-service
gerar PaymentAuthorized ou PaymentRejected
publicar evento de pagamento via outbox
consumir PaymentAuthorized no billing-service
gerar fatura
publicar InvoiceGenerated via outbox
consumir InvoiceGenerated no notifications-service
registrar notificação
```

### Critérios de confiabilidade

```text
duplicidade de POST /orders não cria pedido duplicado
duplicidade de OrderCreated não cria pagamento duplicado
duplicidade de PaymentAuthorized não cria fatura duplicada
duplicidade de InvoiceGenerated não cria notificação duplicada
evento pendente na outbox é publicado após recuperação
evento publicado mais de uma vez não corrompe o estado
mensagem inválida vai para DLQ
```

### Critérios de teste

O projeto deve possuir testes automatizados para:

```text
criação de pedido grava OrderCreated na outbox
publicador envia eventos pendentes
publicador não marca como PUBLISHED quando broker falha
consumidor ignora mensagem já processada
consumidor não duplica pagamento
consumidor não duplica fatura
requisição HTTP duplicada retorna comportamento idempotente
mensagem inválida vai para DLQ
fluxo feliz completo termina com fatura e notificação
```

Testes de integração devem usar dependências reais via containers:

```text
PostgreSQL
Kafka
```

Mocks são aceitáveis em testes unitários, mas não devem substituir os testes de integração principais.

## Estrutura sugerida do repositório

```text
transactional-outbox-idempotency-lab/
  README.md
  docker-compose.yml
  Makefile
  docs/
    architecture.md
    consistency-model.md
    failure-scenarios.md
    event-contracts.md
    idempotency.md
    testing-strategy.md
    trade-offs.md
  services/
    orders-service/
      Dockerfile
      build.gradle.kts
      src/
        main/
        test/
    payments-service/
      Dockerfile
      build.gradle.kts
      src/
        main/
        test/
    billing-service/
      Dockerfile
      build.gradle.kts
      src/
        main/
        test/
    notifications-service/
      Dockerfile
      build.gradle.kts
      src/
        main/
        test/
  infra/
    kafka/
    postgres/
    scripts/
  postman/
    transactional-outbox-lab.postman_collection.json
```

Se quiser reduzir complexidade de build, use um monorepo Gradle multi-module:

```text
settings.gradle.kts
build.gradle.kts
services/
  orders-service/
  payments-service/
  billing-service/
  notifications-service/
libs/
  event-contracts/
  test-support/
```

A biblioteca `event-contracts` pode conter contratos compartilhados dos eventos, com cuidado para não criar acoplamento excessivo de domínio entre os serviços.

## Comandos esperados

O repositório deve suportar comandos simples:

```text
make up
make test
make e2e
make down
make logs
make reset
```

Mesmo que internamente esses comandos chamem Gradle, Docker Compose ou scripts shell, o avaliador deve conseguir rodar a PoC sem estudar a estrutura inteira do projeto.

## Documentação obrigatória

O README principal deve conter:

```text
descrição do problema
diagrama da arquitetura
stack utilizada
como rodar localmente
como executar testes
como disparar o fluxo feliz
como simular falhas
quais garantias a PoC oferece
quais garantias ela não oferece
principais trade-offs
```

Arquivos adicionais recomendados:

```text
docs/architecture.md
docs/consistency-model.md
docs/idempotency.md
docs/failure-scenarios.md
docs/event-contracts.md
docs/trade-offs.md
docs/testing-strategy.md
```

## Demonstrações que devem aparecer no README final

O README da implementação deve conter uma seção de cenários de falha demonstrados. Para cada cenário, registre:

```text
comando executado
estado antes
falha simulada
estado depois
evidência de que não houve duplicidade de negócio
```

Exemplo de formato:

```text
Scenario: duplicated OrderCreated event

Given:
- one order exists
- one OrderCreated event was published

When:
- the same OrderCreated event is published twice

Then:
- payments-service receives both messages
- only one payment row is created
- processed_messages stores the event for payments-consumer
- the duplicate message is acknowledged without side effects
```

## Estratégia de testes

### Testes unitários

Devem cobrir:

```text
cálculo de valor do pedido
validação de comando
decisão de aprovação/rejeição de pagamento
mapeamento de eventos
regras de status
```

### Testes de integração

Devem cobrir:

```text
persistência com PostgreSQL real
constraints de unicidade
inserção em outbox dentro da mesma transação
idempotência por requestId
idempotência por processed_messages
idempotência por chave de negócio
```

### Testes de mensageria

Devem cobrir:

```text
publicação de eventos da outbox para Kafka
consumo de eventos Kafka
redelivery simulada
mensagem duplicada
mensagem inválida
DLQ
```

### Testes end-to-end

Devem cobrir:

```text
POST /orders
aguardar processamento assíncrono
consultar pagamento
consultar fatura
consultar notificação
verificar que o fluxo fechou corretamente
```

Use Awaitility para aguardar processamento assíncrono sem sleeps fixos e frágeis.

## Cenários de teste nomeados

Sugestões de nomes de testes:

```text
CreateOrderShouldPersistOrderAndOutboxEventTest
CreateOrderShouldBeIdempotentByRequestIdTest
OutboxPublisherShouldPublishPendingEventsTest
OutboxPublisherShouldKeepEventPendingWhenBrokerFailsTest
OutboxPublisherMayPublishDuplicateWhenStatusUpdateFailsTest
PaymentConsumerShouldIgnoreAlreadyProcessedMessageTest
PaymentConsumerShouldNotCreateDuplicatePaymentForSameOrderTest
BillingConsumerShouldNotCreateDuplicateInvoiceForSameOrderTest
NotificationConsumerShouldNotSendDuplicateNotificationTest
InvalidEventShouldBeSentToDeadLetterTopicTest
EndToEndFlowShouldCreateOrderPaymentInvoiceAndNotificationTest
RejectedPaymentShouldNotGenerateInvoiceTest
```

Esses nomes deixam claro para o avaliador técnico o que a PoC está provando.

## Observabilidade mínima

Mesmo não sendo uma PoC de observabilidade, o projeto deve ter logs úteis.

Cada log relevante deve conter:

```text
correlationId
eventId
eventType
aggregateId
serviceName
consumerName quando aplicável
```

Exemplos de eventos logados:

```text
order created
outbox event stored
outbox event published
message consumed
message ignored as duplicate
payment authorized
payment rejected
invoice generated
notification registered
message sent to DLQ
```

Evite logs genéricos como:

```text
processing message
success
error occurred
```

Prefira logs semanticamente úteis:

```text
OrderCreated consumed successfully by payments-consumer
Duplicate OrderCreated ignored by payments-consumer
PaymentAuthorized event stored in payments outbox
InvoiceGenerated published from billing outbox
```

## Decisões técnicas que devem estar explícitas

### Por que usar Transactional Outbox

Porque o serviço precisa persistir uma alteração de negócio e garantir que um evento correspondente será eventualmente publicado, sem depender de transação distribuída entre banco e broker.

### Por que aceitar duplicidade

Porque, em sistemas com entrega pelo menos uma vez, duplicidade é uma consequência normal de retries, redelivery e falhas entre publicação, processamento e confirmação.

### Por que consumidores precisam ser idempotentes

Porque o broker pode entregar a mesma mensagem mais de uma vez e porque o publicador de outbox também pode republicar um evento se falhar antes de atualizar o status local.

### Por que `processed_messages` não basta

Porque `processed_messages` protege contra repetição da mesma mensagem, mas não necessariamente contra repetição da mesma intenção de negócio representada por mensagens diferentes. Por isso, também são necessárias constraints de negócio.

### Por que não usar exactly-once como promessa principal

Porque a PoC deve ser honesta sobre as garantias que realmente controla no nível da aplicação. A garantia prática buscada é que os efeitos de negócio sejam idempotentes e consistentes mesmo sob duplicidade.

### Por que não chamar serviços síncronos durante o consumo

Porque consumidores que dependem de chamadas síncronas para outros serviços aumentam acoplamento, latência e risco de falhas em cascata. Os eventos devem carregar dados suficientes para que o consumidor execute sua responsabilidade local.

## Roadmap de implementação recomendado

### Fase 1 - Base do monorepo

Entregar:

```text
estrutura Gradle multi-module
orders-service
payments-service
billing-service
docker-compose com PostgreSQL e Kafka
Flyway em cada serviço
health checks básicos
```

Resultado esperado:

```text
serviços sobem localmente
bancos são criados
migrations rodam
testes básicos passam
```

### Fase 2 - Orders Service com outbox

Entregar:

```text
POST /orders
GET /orders/{id}
orders table
outbox_events table
idempotência por requestId
OrderCreated gravado na outbox na mesma transação
```

Resultado esperado:

```text
pedido criado gera evento pendente na outbox
requisição duplicada não cria pedido duplicado
```

### Fase 3 - Outbox Publisher

Entregar:

```text
job agendado ou worker contínuo
busca de eventos pendentes
publicação em Kafka
marcação como PUBLISHED
attempt_count
last_error
```

Resultado esperado:

```text
OrderCreated é publicado no tópico orders.events
falha de broker não perde evento
```

### Fase 4 - Payments Service idempotente

Entregar:

```text
consumer de OrderCreated
processed_messages
payments table
PaymentAuthorized
PaymentRejected
outbox no payments-service
constraints por orderId
```

Resultado esperado:

```text
OrderCreated gera pagamento
duplicidade de OrderCreated não gera pagamento duplicado
```

### Fase 5 - Billing Service idempotente

Entregar:

```text
consumer de PaymentAuthorized
processed_messages
invoices table
InvoiceGenerated
outbox no billing-service
constraints por orderId e paymentId
```

Resultado esperado:

```text
PaymentAuthorized gera fatura
duplicidade de PaymentAuthorized não gera fatura duplicada
```

### Fase 6 - Notifications Service

Entregar:

```text
consumer de InvoiceGenerated
processed_messages
notifications table
constraint por invoiceId e template
```

Resultado esperado:

```text
InvoiceGenerated gera notificação
duplicidade não gera notificação duplicada
```

### Fase 7 - DLQ e mensagens inválidas

Entregar:

```text
validação de envelope
validação de payload
tratamento de eventVersion desconhecida
envio para DLQ
registro de erro
```

Resultado esperado:

```text
mensagem inválida não trava consumidor
mensagem inválida não gera efeito de negócio
mensagem inválida fica rastreável
```

### Fase 8 - Testes de falha e documentação

Entregar:

```text
testes de integração com PostgreSQL e Kafka
teste de redelivery
teste de duplicidade
teste de broker indisponível
teste de DLQ
docs/failure-scenarios.md
docs/idempotency.md
docs/trade-offs.md
```

Resultado esperado:

```text
avaliador consegue rodar testes e entender as garantias do sistema
```

## Escopo mínimo aceitável

Se quiser reduzir para uma primeira versão publicável, implemente apenas:

```text
orders-service
payments-service
PostgreSQL
Kafka
outbox no orders-service
processed_messages no payments-service
idempotência por requestId
idempotência por eventId
idempotência por orderId
teste de mensagem duplicada
teste de queda antes da publicação
teste de fluxo feliz
```

Essa versão já demonstra o núcleo do problema.

Depois evolua para:

```text
billing-service
notifications-service
DLQ
falhas transitórias
documentação avançada
observabilidade
```

## Escopo ideal para portfólio

A versão ideal para fixar no GitHub deve conter:

```text
três ou quatro serviços
outbox em todos os serviços que publicam eventos
processed_messages em todos os serviços que consomem eventos
Kafka com tópicos separados
DLQ
PostgreSQL por serviço
migrations versionadas
testes de integração com Testcontainers
README com diagramas
documentação de falhas
documentação de idempotência
Makefile com comandos simples
CI no GitHub Actions
```

Essa versão comunica nível de backend pleno forte.

## Resultado esperado no portfólio

Ao final, o repositório deve demonstrar que o autor não apenas sabe usar mensageria, mas entende os problemas reais que aparecem quando mensageria entra em produção:

```text
evento perdido
evento duplicado
consumidor que cai no momento errado
cliente que repete requisição
broker indisponível
mensagem inválida
reprocessamento
efeito colateral duplicado
consistência eventual
```

Essa PoC deve ser posicionada como um laboratório de confiabilidade transacional em sistemas distribuídos, não como um exemplo básico de Kafka.

O diferencial do projeto será a clareza com que ele prova a seguinte ideia:

```text
em sistemas distribuídos, a pergunta correta não é "como garanto que a mensagem nunca será duplicada?";
a pergunta correta é "o que acontece quando ela for duplicada?".
```
