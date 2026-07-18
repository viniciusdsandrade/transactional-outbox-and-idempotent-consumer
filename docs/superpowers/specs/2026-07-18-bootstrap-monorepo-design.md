# Bootstrap do monorepo — desenho

## Contexto e checkpoint

O repositório contém somente o enunciado e a licença. O checkpoint anterior ao primeiro diff é a branch `main` limpa no commit `cfb548a6136a10ffd41072a0c3e13dbabfb735d7`, em paridade com `origin/main`.

O pedido é preparar o código sem implementar a PoC funcional. O README já define essa fronteira na seção **Fase 1 — Base do monorepo**: Gradle multi-module, `orders-service`, `payments-service`, `billing-service`, PostgreSQL, Kafka, Flyway, health checks e testes básicos.

## Alternativas consideradas

### 1. Três serviços da Fase 1 — adotada

Criar os três módulos citados literalmente no roadmap, cada um como aplicação Spring Boot Java 21 independente. Essa opção entrega a base que o README pede e adia corretamente notificações e todo o fluxo de negócio.

### 2. Quatro serviços desde o bootstrap

Adicionar também `notifications-service` aproximaria a topologia da versão ideal, mas ultrapassaria a lista da Fase 1 e aumentaria o build sem produzir comportamento útil agora.

### 3. Somente orders e payments

É a menor topologia do “escopo mínimo aceitável” final, mas não satisfaz a Fase 1, que inclui `billing-service`, e exigiria churn estrutural logo na etapa seguinte.

## Decisões

- Linguagem das aplicações: Java 21.
- Build: Gradle Kotlin DSL, multi-module, com wrapper Gradle 9.5.1.
- Framework: Spring Boot 4.1.0.
- Serviços iniciais: `orders-service`, `payments-service` e `billing-service`.
- Banco local: uma instância PostgreSQL 17 com três bancos lógicos isolados: `orders_db`, `payments_db` e `billing_db`.
- Broker local: Apache Kafka 4.3.1 em modo KRaft, com listener externo para o host e listener interno para os containers.
- Migrações: uma migration Flyway baseline, sem tabelas de domínio, em cada serviço.
- Saúde: Actuator `/actuator/health` em cada serviço e health checks do Compose.
- Testes: um teste de contexto por serviço usando PostgreSQL real via Testcontainers, com asserção de que a baseline Flyway foi aplicada.
- Operação: `Makefile` para build, teste, subida, smoke, logs, parada e reset do ambiente.

As versões de Spring Boot e Gradle seguem a combinação publicada pelo Spring Initializr em 18 de julho de 2026. PostgreSQL e Kafka usam tags oficiais estáveis e explícitas.

## Arquitetura do bootstrap

```text
Gradle root
├── services/orders-service       -> Java 21 + Spring Boot + banco orders_db
├── services/payments-service     -> Java 21 + Spring Boot + banco payments_db
└── services/billing-service      -> Java 21 + Spring Boot + banco billing_db

Docker Compose
├── postgres                      -> cria os três bancos lógicos
├── kafka                         -> broker KRaft de desenvolvimento
├── orders-service                -> porta 8081
├── payments-service              -> porta 8082
└── billing-service               -> porta 8083
```

Não haverá biblioteca compartilhada no bootstrap. Contratos de eventos e suporte de testes compartilhado só serão extraídos quando existirem casos concretos que justifiquem a abstração.

## Inicialização e fluxo de dados

1. O PostgreSQL inicia e executa o script que cria os três bancos.
2. O Kafka inicia e fica acessível em `localhost:9092` para processos no host e em `kafka:19092` para containers.
3. Cada aplicação conecta somente ao seu banco lógico.
4. O Flyway aplica `V1__baseline.sql`; a migration apenas estabelece o histórico versionado e não cria modelo de negócio.
5. O contexto Spring sobe e o Actuator informa saúde em sua porta.

Não existe fluxo de eventos nesta etapa. A presença da dependência Kafka prepara o classpath e a configuração, mas nenhum tópico, producer, consumer ou listener será criado.

## Configuração e falhas de bootstrap

As configurações usam variáveis padrão do Spring (`SPRING_DATASOURCE_*` e `SPRING_KAFKA_BOOTSTRAP_SERVERS`) para permitir execução local ou em container sem perfis duplicados.

O Compose só considera cada aplicação saudável depois que o endpoint Actuator retorna `UP`. As aplicações dependem da saúde do PostgreSQL e do Kafka. Uma falha de conexão impede o ambiente de ser reportado como pronto, em vez de produzir um falso positivo.

O alvo `make reset` remove containers e volumes locais. Essa ação será documentada explicitamente como destrutiva apenas para dados descartáveis do laboratório.

## Estratégia de testes

Cada módulo terá um teste de contexto que:

1. inicia um PostgreSQL real com Testcontainers;
2. deixa o Spring Boot configurar o datasource por `@ServiceConnection`;
3. carrega a aplicação completa;
4. confirma que a migration Flyway de versão `1` foi aplicada.

Kafka não será iniciado nos testes de contexto porque ainda não existe comportamento que publique ou consuma mensagens. O broker será validado no smoke do Compose. Isso mantém a prova proporcional ao risco da Fase 1.

Além dos testes, a verificação final inclui:

- `./gradlew test`;
- `./gradlew bootJar`;
- `docker compose config --quiet`;
- `docker compose up --build --detach --wait`;
- resposta `UP` nos três endpoints Actuator;
- presença da migration `1` nos três históricos Flyway;
- revisão do diff para confirmar ausência de implementação funcional.

## Fronteira explícita

Este bootstrap não conterá:

- controllers ou endpoints de negócio;
- entidades, repositories ou services de domínio;
- tabelas `orders`, `payments`, `invoices`, `outbox_events` ou `processed_messages`;
- contratos como `OrderCreated`, `PaymentAuthorized`, `PaymentRejected` ou `InvoiceGenerated`;
- producers, consumers, schedulers, polling de outbox ou confirmação de offset;
- idempotência HTTP, de mensagem ou de negócio;
- retry, backoff, DLQ ou reprocessamento;
- `notifications-service`;
- teste end-to-end do fluxo distribuído.

Esses itens pertencem às fases funcionais seguintes. O README será atualizado para declarar de forma inequívoca que o repositório está somente na Fase 1.

## Critérios de aceite

- O wrapper lista três subprojetos e o build gera três jars executáveis.
- Os três testes de contexto passam com PostgreSQL real.
- O Compose cria PostgreSQL, Kafka e os três serviços sem configuração manual.
- Os bancos `orders_db`, `payments_db` e `billing_db` existem.
- O Flyway registra a migration `1` em cada banco.
- Os endpoints `8081`, `8082` e `8083` respondem `UP` em `/actuator/health`.
- O diff não implementa nenhum item funcional das Fases 2 a 8.
