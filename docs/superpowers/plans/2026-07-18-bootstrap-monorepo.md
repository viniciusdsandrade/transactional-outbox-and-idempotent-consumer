# Phase 1 Monorepo Bootstrap Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Entregar somente a base executável e testável da Fase 1 descrita no README, sem implementar o fluxo funcional da PoC.

**Architecture:** Um monorepo Gradle contém três aplicações Spring Boot Java 21 independentes. Uma composição local fornece Kafka e uma instância PostgreSQL com três bancos lógicos; cada aplicação executa sua própria baseline Flyway e expõe somente o health check Actuator.

**Tech Stack:** Java 21, Gradle 9.5.1 Kotlin DSL, Spring Boot 4.1.0, PostgreSQL 17.10, Apache Kafka 4.3.1, Flyway, Docker Compose, Testcontainers e JUnit 5.

## Global Constraints

- Checkpoint: `main` limpa em `cfb548a6136a10ffd41072a0c3e13dbabfb735d7`, inicialmente igual a `origin/main`.
- Criar exatamente `orders-service`, `payments-service` e `billing-service` nesta fase.
- Não criar `notifications-service` nesta fase.
- Não criar controller, entidade, repository, service de domínio, contrato de evento, producer, consumer ou scheduler.
- Não criar tabelas de domínio, `outbox_events` ou `processed_messages`.
- Não implementar idempotência, retry, DLQ, publicação, consumo ou fluxo end-to-end.
- Usar PostgreSQL real nos testes que provam datasource e Flyway; não substituir por H2.
- Preservar o enunciado existente no README e acrescentar somente instruções honestas sobre o estado do bootstrap.
- Publicar os commits diretamente em `origin/main` somente depois de toda a verificação final.

---

### Task 1: Build Gradle multi-module reproduzível

**Files:**
- Create: `.editorconfig`
- Create: `.gitattributes`
- Create: `.gitignore`
- Create: `settings.gradle.kts`
- Create: `build.gradle.kts`
- Create: `gradle.properties`
- Create: `gradlew`
- Create: `gradlew.bat`
- Create: `gradle/wrapper/gradle-wrapper.jar`
- Create: `gradle/wrapper/gradle-wrapper.properties`
- Create: `services/orders-service/build.gradle.kts`
- Create: `services/payments-service/build.gradle.kts`
- Create: `services/billing-service/build.gradle.kts`

**Interfaces:**
- Consumes: Java 21 instalado e Maven Central acessível.
- Produces: tarefas Gradle `:services:orders-service:*`, `:services:payments-service:*` e `:services:billing-service:*` com dependências homogêneas.

- [ ] **Step 1: Gerar e fixar o Gradle Wrapper**

Gerar o wrapper 9.5.1 e manter a distribuição binária:

```bash
gradle wrapper --gradle-version 9.5.1 --distribution-type bin
./gradlew wrapper --gradle-version 9.5.1 --distribution-type bin
```

O `gradle/wrapper/gradle-wrapper.properties` deve apontar para:

```properties
distributionUrl=https\://services.gradle.org/distributions/gradle-9.5.1-bin.zip
networkTimeout=10000
validateDistributionUrl=true
```

- [ ] **Step 2: Declarar os módulos e o build comum**

Criar `settings.gradle.kts`:

```kotlin
rootProject.name = "transactional-outbox-idempotency-lab"

include(
    "services:orders-service",
    "services:payments-service",
    "services:billing-service",
)
```

Criar `.editorconfig`:

```ini
root = true

[*]
charset = utf-8
end_of_line = lf
insert_final_newline = true
indent_style = space
indent_size = 4
trim_trailing_whitespace = true

[*.{yml,yaml}]
indent_size = 2

[Makefile]
indent_style = tab
```

Criar `.gitattributes`:

```gitattributes
* text=auto eol=lf
*.bat text eol=crlf
*.jar binary
```

Criar `.gitignore`:

```gitignore
.gradle/
**/build/
.idea/
*.iml
.vscode/
.DS_Store
```

Criar `gradle.properties`:

```properties
org.gradle.caching=true
org.gradle.configuration-cache=true
org.gradle.parallel=true
org.gradle.jvmargs=-Xmx1g -Dfile.encoding=UTF-8
```

Criar `build.gradle.kts` com plugins e dependências comuns:

```kotlin
import org.gradle.api.plugins.JavaPluginExtension
import org.gradle.api.tasks.testing.Test
import org.gradle.jvm.toolchain.JavaLanguageVersion
import org.springframework.boot.gradle.tasks.bundling.BootJar

plugins {
    id("org.springframework.boot") version "4.1.0" apply false
    id("io.spring.dependency-management") version "1.1.7" apply false
}

allprojects {
    group = "io.github.viniciusdsandrade.outbox"
    version = "0.0.1-SNAPSHOT"
    repositories { mavenCentral() }
}

subprojects {
    apply(plugin = "java")
    apply(plugin = "org.springframework.boot")
    apply(plugin = "io.spring.dependency-management")

    extensions.configure<JavaPluginExtension> {
        toolchain.languageVersion.set(JavaLanguageVersion.of(21))
    }

    dependencies {
        "implementation"("org.springframework.boot:spring-boot-starter-actuator")
        "implementation"("org.springframework.boot:spring-boot-starter-data-jpa")
        "implementation"("org.springframework.boot:spring-boot-starter-flyway")
        "implementation"("org.springframework.boot:spring-boot-starter-kafka")
        "implementation"("org.springframework.boot:spring-boot-starter-webmvc")
        "implementation"("org.flywaydb:flyway-database-postgresql")
        "runtimeOnly"("org.postgresql:postgresql")

        "testImplementation"("org.springframework.boot:spring-boot-starter-actuator-test")
        "testImplementation"("org.springframework.boot:spring-boot-starter-data-jpa-test")
        "testImplementation"("org.springframework.boot:spring-boot-starter-flyway-test")
        "testImplementation"("org.springframework.boot:spring-boot-starter-kafka-test")
        "testImplementation"("org.springframework.boot:spring-boot-starter-webmvc-test")
        "testImplementation"("org.springframework.boot:spring-boot-testcontainers")
        "testImplementation"("org.testcontainers:testcontainers-junit-jupiter")
        "testImplementation"("org.testcontainers:testcontainers-postgresql")
        "testRuntimeOnly"("org.junit.platform:junit-platform-launcher")
    }

    tasks.withType<Test>().configureEach { useJUnitPlatform() }
    tasks.withType<BootJar>().configureEach {
        archiveFileName.set("${project.name}.jar")
    }
}
```

Criar `services/orders-service/build.gradle.kts`:

```kotlin
description = "Orders service bootstrap"
```

Criar `services/payments-service/build.gradle.kts`:

```kotlin
description = "Payments service bootstrap"
```

Criar `services/billing-service/build.gradle.kts`:

```kotlin
description = "Billing service bootstrap"
```

- [ ] **Step 3: Validar a estrutura Gradle**

Run:

```bash
./gradlew projects
```

Expected: exit `0` e os projetos `:services:orders-service`, `:services:payments-service` e `:services:billing-service` listados.

- [ ] **Step 4: Commit do build base**

```bash
git add .editorconfig .gitattributes .gitignore settings.gradle.kts build.gradle.kts gradle.properties gradlew gradlew.bat gradle services/*/build.gradle.kts
git commit -m "build: initialize Gradle monorepo"
```

---

### Task 2: Especificar por testes o bootstrap de cada aplicação

**Files:**
- Create: `services/orders-service/src/test/java/io/github/viniciusdsandrade/outbox/orders/PostgresTestConfiguration.java`
- Create: `services/orders-service/src/test/java/io/github/viniciusdsandrade/outbox/orders/OrdersServiceApplicationTest.java`
- Create: `services/payments-service/src/test/java/io/github/viniciusdsandrade/outbox/payments/PostgresTestConfiguration.java`
- Create: `services/payments-service/src/test/java/io/github/viniciusdsandrade/outbox/payments/PaymentsServiceApplicationTest.java`
- Create: `services/billing-service/src/test/java/io/github/viniciusdsandrade/outbox/billing/PostgresTestConfiguration.java`
- Create: `services/billing-service/src/test/java/io/github/viniciusdsandrade/outbox/billing/BillingServiceApplicationTest.java`

**Interfaces:**
- Consumes: dependências Testcontainers e Spring Boot Test configuradas na Task 1.
- Produces: três testes que exigem uma aplicação Spring Boot localizável e uma migration Flyway aplicada na versão `1`.

- [ ] **Step 1: Criar a configuração PostgreSQL real de teste**

Criar a configuração isolada de orders:

```java
package io.github.viniciusdsandrade.outbox.orders;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

@TestConfiguration(proxyBeanMethods = false)
class PostgresTestConfiguration {

    @Bean
    @ServiceConnection
    PostgreSQLContainer postgresContainer() {
        return new PostgreSQLContainer(
                DockerImageName.parse("postgres:17.10-alpine3.23")
        );
    }
}
```

Criar a configuração isolada de payments:

```java
package io.github.viniciusdsandrade.outbox.payments;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

@TestConfiguration(proxyBeanMethods = false)
class PostgresTestConfiguration {

    @Bean
    @ServiceConnection
    PostgreSQLContainer postgresContainer() {
        return new PostgreSQLContainer(
                DockerImageName.parse("postgres:17.10-alpine3.23")
        );
    }
}
```

Criar a configuração isolada de billing:

```java
package io.github.viniciusdsandrade.outbox.billing;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

@TestConfiguration(proxyBeanMethods = false)
class PostgresTestConfiguration {

    @Bean
    @ServiceConnection
    PostgreSQLContainer postgresContainer() {
        return new PostgreSQLContainer(
                DockerImageName.parse("postgres:17.10-alpine3.23")
        );
    }
}
```

- [ ] **Step 2: Criar os três testes de migration**

Criar o teste de orders:

```java
package io.github.viniciusdsandrade.outbox.orders;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

import static org.assertj.core.api.Assertions.assertThat;

@Import(PostgresTestConfiguration.class)
@SpringBootTest
class OrdersServiceApplicationTest {

    @Autowired
    private Flyway flyway;

    @Test
    void appliesBootstrapMigration() {
        assertThat(flyway.info().current()).isNotNull();
        assertThat(flyway.info().current().getVersion().getVersion()).isEqualTo("1");
    }
}
```

Criar o teste de payments:

```java
package io.github.viniciusdsandrade.outbox.payments;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

import static org.assertj.core.api.Assertions.assertThat;

@Import(PostgresTestConfiguration.class)
@SpringBootTest
class PaymentsServiceApplicationTest {

    @Autowired
    private Flyway flyway;

    @Test
    void appliesBootstrapMigration() {
        assertThat(flyway.info().current()).isNotNull();
        assertThat(flyway.info().current().getVersion().getVersion()).isEqualTo("1");
    }
}
```

Criar o teste de billing:

```java
package io.github.viniciusdsandrade.outbox.billing;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

import static org.assertj.core.api.Assertions.assertThat;

@Import(PostgresTestConfiguration.class)
@SpringBootTest
class BillingServiceApplicationTest {

    @Autowired
    private Flyway flyway;

    @Test
    void appliesBootstrapMigration() {
        assertThat(flyway.info().current()).isNotNull();
        assertThat(flyway.info().current().getVersion().getVersion()).isEqualTo("1");
    }
}
```

- [ ] **Step 3: Confirmar RED**

Run:

```bash
./gradlew test
```

Expected: os testes falham porque nenhuma classe `@SpringBootConfiguration` existe ainda. A falha deve indicar que o bootstrap de aplicação está ausente, e não erro de sintaxe no teste.

---

### Task 3: Implementar o mínimo para os testes passarem

**Files:**
- Create: `services/orders-service/src/main/java/io/github/viniciusdsandrade/outbox/orders/OrdersServiceApplication.java`
- Create: `services/orders-service/src/main/resources/application.yml`
- Create: `services/orders-service/src/main/resources/db/migration/V1__baseline.sql`
- Create: `services/payments-service/src/main/java/io/github/viniciusdsandrade/outbox/payments/PaymentsServiceApplication.java`
- Create: `services/payments-service/src/main/resources/application.yml`
- Create: `services/payments-service/src/main/resources/db/migration/V1__baseline.sql`
- Create: `services/billing-service/src/main/java/io/github/viniciusdsandrade/outbox/billing/BillingServiceApplication.java`
- Create: `services/billing-service/src/main/resources/application.yml`
- Create: `services/billing-service/src/main/resources/db/migration/V1__baseline.sql`

**Interfaces:**
- Consumes: testes da Task 2 e variáveis Spring `SPRING_DATASOURCE_*`/`SPRING_KAFKA_BOOTSTRAP_SERVERS`.
- Produces: três aplicações vazias com jars executáveis e health endpoints; nenhum endpoint de negócio.

- [ ] **Step 1: Criar os entrypoints Spring Boot**

Criar orders:

```java
package io.github.viniciusdsandrade.outbox.orders;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class OrdersServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(OrdersServiceApplication.class, args);
    }
}
```

Criar payments:

```java
package io.github.viniciusdsandrade.outbox.payments;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class PaymentsServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(PaymentsServiceApplication.class, args);
    }
}
```

Criar billing:

```java
package io.github.viniciusdsandrade.outbox.billing;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class BillingServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(BillingServiceApplication.class, args);
    }
}
```

- [ ] **Step 2: Configurar cada serviço**

Orders usa:

```yaml
server:
  port: 8081
spring:
  application:
    name: orders-service
  datasource:
    url: jdbc:postgresql://localhost:5432/orders_db
    username: lab
    password: lab
  flyway:
    validate-migration-naming: true
  jpa:
    hibernate:
      ddl-auto: validate
    open-in-view: false
  kafka:
    bootstrap-servers: localhost:9092
management:
  endpoints:
    web:
      exposure:
        include: health,info
  endpoint:
    health:
      probes:
        enabled: true
```

Payments usa:

```yaml
server:
  port: 8082
spring:
  application:
    name: payments-service
  datasource:
    url: jdbc:postgresql://localhost:5432/payments_db
    username: lab
    password: lab
  flyway:
    validate-migration-naming: true
  jpa:
    hibernate:
      ddl-auto: validate
    open-in-view: false
  kafka:
    bootstrap-servers: localhost:9092
management:
  endpoints:
    web:
      exposure:
        include: health,info
  endpoint:
    health:
      probes:
        enabled: true
```

Billing usa:

```yaml
server:
  port: 8083
spring:
  application:
    name: billing-service
  datasource:
    url: jdbc:postgresql://localhost:5432/billing_db
    username: lab
    password: lab
  flyway:
    validate-migration-naming: true
  jpa:
    hibernate:
      ddl-auto: validate
    open-in-view: false
  kafka:
    bootstrap-servers: localhost:9092
management:
  endpoints:
    web:
      exposure:
        include: health,info
  endpoint:
    health:
      probes:
        enabled: true
```

- [ ] **Step 3: Criar baselines sem modelo funcional**

Cada `V1__baseline.sql` contém:

```sql
-- Establishes Flyway history for the bootstrap only.
-- Business and messaging tables belong to later phases.
SELECT 1;
```

- [ ] **Step 4: Confirmar GREEN**

Run:

```bash
./gradlew test
```

Expected: três testes executados e nenhum teste falhando.

- [ ] **Step 5: Gerar jars executáveis**

Run:

```bash
./gradlew bootJar
```

Expected: `orders-service.jar`, `payments-service.jar` e `billing-service.jar` nos respectivos diretórios `build/libs`.

- [ ] **Step 6: Commit das aplicações vazias**

```bash
git add services
git commit -m "feat: bootstrap Spring services"
```

---

### Task 4: Infraestrutura local e imagens dos serviços

**Files:**
- Create: `.dockerignore`
- Create: `docker-compose.yml`
- Create: `infra/postgres/init-databases.sql`
- Create: `services/orders-service/Dockerfile`
- Create: `services/payments-service/Dockerfile`
- Create: `services/billing-service/Dockerfile`
- Create: `Makefile`

**Interfaces:**
- Consumes: os três jars construíveis pela Task 3.
- Produces: `make up`, `make smoke`, `make logs`, `make down` e `make reset`; portas 8081–8083 e Kafka externo em 9092.

- [ ] **Step 0: Limitar o contexto de build das imagens**

Criar `.dockerignore`:

```dockerignore
.git
.gradle
**/build
.idea
.vscode
docs
infra
README.md
LICENSE
docker-compose.yml
Makefile
```

- [ ] **Step 1: Criar os bancos lógicos**

`infra/postgres/init-databases.sql`:

```sql
CREATE DATABASE orders_db;
CREATE DATABASE payments_db;
CREATE DATABASE billing_db;
```

- [ ] **Step 2: Criar Dockerfiles mínimos**

`services/orders-service/Dockerfile`:

```dockerfile
FROM eclipse-temurin:21-jdk-alpine AS build
WORKDIR /workspace
COPY . .
RUN ./gradlew :services:orders-service:bootJar --no-daemon

FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY --from=build /workspace/services/orders-service/build/libs/orders-service.jar app.jar
EXPOSE 8081
ENTRYPOINT ["java", "-jar", "app.jar"]
```

`services/payments-service/Dockerfile`:

```dockerfile
FROM eclipse-temurin:21-jdk-alpine AS build
WORKDIR /workspace
COPY . .
RUN ./gradlew :services:payments-service:bootJar --no-daemon

FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY --from=build /workspace/services/payments-service/build/libs/payments-service.jar app.jar
EXPOSE 8082
ENTRYPOINT ["java", "-jar", "app.jar"]
```

`services/billing-service/Dockerfile`:

```dockerfile
FROM eclipse-temurin:21-jdk-alpine AS build
WORKDIR /workspace
COPY . .
RUN ./gradlew :services:billing-service:bootJar --no-daemon

FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY --from=build /workspace/services/billing-service/build/libs/billing-service.jar app.jar
EXPOSE 8083
ENTRYPOINT ["java", "-jar", "app.jar"]
```

- [ ] **Step 3: Criar o Compose**

Criar `docker-compose.yml`:

```yaml
services:
  postgres:
    image: postgres:17.10-alpine3.23
    environment:
      POSTGRES_DB: postgres
      POSTGRES_USER: lab
      POSTGRES_PASSWORD: lab
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./infra/postgres/init-databases.sql:/docker-entrypoint-initdb.d/init-databases.sql:ro
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U lab -d postgres"]
      interval: 5s
      timeout: 5s
      retries: 20
      start_period: 5s

  kafka:
    image: apache/kafka:4.3.1
    hostname: kafka
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT_HOST://localhost:9092,PLAINTEXT://kafka:19092
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:29093
      KAFKA_LISTENERS: CONTROLLER://:29093,PLAINTEXT_HOST://:9092,PLAINTEXT://:19092
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      CLUSTER_ID: 4L6g3nShT-eMCtK--X86sw
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR: 1
      KAFKA_LOG_DIRS: /tmp/kraft-combined-logs
    volumes:
      - kafka_data:/tmp/kraft-combined-logs
    healthcheck:
      test: ["CMD-SHELL", "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1"]
      interval: 5s
      timeout: 10s
      retries: 20
      start_period: 10s

  orders-service:
    build:
      context: .
      dockerfile: services/orders-service/Dockerfile
    environment:
      SPRING_DATASOURCE_URL: jdbc:postgresql://postgres:5432/orders_db
      SPRING_DATASOURCE_USERNAME: lab
      SPRING_DATASOURCE_PASSWORD: lab
      SPRING_KAFKA_BOOTSTRAP_SERVERS: kafka:19092
    ports:
      - "8081:8081"
    depends_on:
      postgres:
        condition: service_healthy
      kafka:
        condition: service_healthy
    healthcheck:
      test: ["CMD-SHELL", "wget -qO- http://localhost:8081/actuator/health | grep -q '\"status\":\"UP\"'"]
      interval: 5s
      timeout: 5s
      retries: 20
      start_period: 10s

  payments-service:
    build:
      context: .
      dockerfile: services/payments-service/Dockerfile
    environment:
      SPRING_DATASOURCE_URL: jdbc:postgresql://postgres:5432/payments_db
      SPRING_DATASOURCE_USERNAME: lab
      SPRING_DATASOURCE_PASSWORD: lab
      SPRING_KAFKA_BOOTSTRAP_SERVERS: kafka:19092
    ports:
      - "8082:8082"
    depends_on:
      postgres:
        condition: service_healthy
      kafka:
        condition: service_healthy
    healthcheck:
      test: ["CMD-SHELL", "wget -qO- http://localhost:8082/actuator/health | grep -q '\"status\":\"UP\"'"]
      interval: 5s
      timeout: 5s
      retries: 20
      start_period: 10s

  billing-service:
    build:
      context: .
      dockerfile: services/billing-service/Dockerfile
    environment:
      SPRING_DATASOURCE_URL: jdbc:postgresql://postgres:5432/billing_db
      SPRING_DATASOURCE_USERNAME: lab
      SPRING_DATASOURCE_PASSWORD: lab
      SPRING_KAFKA_BOOTSTRAP_SERVERS: kafka:19092
    ports:
      - "8083:8083"
    depends_on:
      postgres:
        condition: service_healthy
      kafka:
        condition: service_healthy
    healthcheck:
      test: ["CMD-SHELL", "wget -qO- http://localhost:8083/actuator/health | grep -q '\"status\":\"UP\"'"]
      interval: 5s
      timeout: 5s
      retries: 20
      start_period: 10s

volumes:
  postgres_data:
  kafka_data:
```

- [ ] **Step 4: Criar a interface Make**

`Makefile`:

```makefile
GRADLE := ./gradlew
COMPOSE := docker compose

.PHONY: build test config up smoke logs down reset

build:
	$(GRADLE) bootJar

test:
	$(GRADLE) test

config:
	$(COMPOSE) config --quiet

up:
	$(COMPOSE) up --build --detach --wait

smoke:
	curl --fail --silent --show-error http://localhost:8081/actuator/health
	curl --fail --silent --show-error http://localhost:8082/actuator/health
	curl --fail --silent --show-error http://localhost:8083/actuator/health

logs:
	$(COMPOSE) logs --follow

down:
	$(COMPOSE) down --remove-orphans

reset:
	$(COMPOSE) down --volumes --remove-orphans
```

Não criar um alvo `e2e` que retorne sucesso sem existir fluxo funcional.

- [ ] **Step 5: Validar sintaxe e runtime**

Run:

```bash
docker compose config --quiet
docker compose up --build --detach --wait
make smoke
```

Expected: Compose e smoke retornam exit `0`, com três respostas contendo `"status":"UP"`.

- [ ] **Step 6: Provar bancos e migrations**

Run:

```bash
docker compose exec -T postgres psql -U lab -d postgres -Atc "SELECT datname FROM pg_database WHERE datname IN ('orders_db','payments_db','billing_db') ORDER BY datname;"
docker compose exec -T postgres psql -U lab -d orders_db -Atc 'SELECT version, success FROM flyway_schema_history ORDER BY installed_rank;'
docker compose exec -T postgres psql -U lab -d payments_db -Atc 'SELECT version, success FROM flyway_schema_history ORDER BY installed_rank;'
docker compose exec -T postgres psql -U lab -d billing_db -Atc 'SELECT version, success FROM flyway_schema_history ORDER BY installed_rank;'
```

Expected: três bancos listados e migration `1|t` em cada histórico.

- [ ] **Step 7: Encerrar o runtime sem apagar dados**

```bash
docker compose down --remove-orphans
```

- [ ] **Step 8: Commit da infraestrutura**

```bash
git add .dockerignore docker-compose.yml Makefile infra services/*/Dockerfile
git commit -m "build: add local Kafka and PostgreSQL stack"
```

---

### Task 5: Documentar o estado real do bootstrap

**Files:**
- Modify: `README.md`

**Interfaces:**
- Consumes: comandos e portas validados na Task 4.
- Produces: instruções executáveis sem alegar outbox, idempotência ou fluxo distribuído já implementados.

- [ ] **Step 1: Acrescentar a seção de estado atual**

Logo após o título, inserir:

````markdown
## Estado atual: bootstrap da Fase 1

O repositório contém somente a base executável do monorepo descrita na **Fase 1** do roadmap. Existem três aplicações Spring Boot vazias (`orders-service`, `payments-service` e `billing-service`), três bancos PostgreSQL lógicos, Kafka, migrations baseline do Flyway e health checks.

Ainda não existem endpoints de negócio, tabelas de domínio/outbox, producers, consumers, idempotência, retries, DLQ ou fluxo end-to-end. Esses comportamentos pertencem às próximas fases do roadmap.

### Pré-requisitos

- Java 21;
- Docker com Docker Compose;
- GNU Make.

O Gradle não precisa estar instalado: o wrapper está versionado no repositório.

### Comandos do bootstrap

```bash
make test    # executa os testes com PostgreSQL real via Testcontainers
make build   # gera os três jars executáveis
make up      # constrói e inicia toda a stack, aguardando os health checks
make smoke   # consulta os três endpoints Actuator
make logs    # acompanha os logs da stack
make down    # encerra os containers e preserva os volumes
make reset   # encerra os containers e remove os volumes locais
```

`make reset` apaga definitivamente apenas os dados locais descartáveis deste laboratório.

### Portas locais

| Componente | Porta | Verificação |
| --- | ---: | --- |
| orders-service | 8081 | `GET /actuator/health` |
| payments-service | 8082 | `GET /actuator/health` |
| billing-service | 8083 | `GET /actuator/health` |
| Kafka | 9092 | bootstrap server para processos no host |
| PostgreSQL | 5432 | bancos `orders_db`, `payments_db` e `billing_db` |

As credenciais PostgreSQL `lab`/`lab` são exclusivamente locais e descartáveis.
````

- [ ] **Step 2: Validar Markdown e diff**

Run:

```bash
git diff --check
git diff -- README.md
```

Expected: nenhum erro de whitespace e texto coerente com a fronteira do desenho.

- [ ] **Step 3: Commit da documentação operacional**

```bash
git add README.md
git commit -m "docs: explain bootstrap workflow"
```

---

### Task 6: Auditoria final, publicação e paridade remota

**Files:**
- Verify only: todos os arquivos rastreados.

**Interfaces:**
- Consumes: todos os commits das Tasks 1–5.
- Produces: `origin/main` apontando para o mesmo SHA validado localmente.

- [ ] **Step 1: Reexecutar a validação completa e fresca**

```bash
./gradlew clean test bootJar
docker compose config --quiet
docker compose up --build --detach --wait
make smoke
```

Expected: todos os comandos retornam exit `0`.

- [ ] **Step 2: Auditar o limite funcional**

```bash
git grep -n -E '@(RestController|Controller|KafkaListener|Entity)|outbox_events|processed_messages|OrderCreated|PaymentAuthorized|InvoiceGenerated'
```

Expected: nenhuma ocorrência em código produtivo ou migrations; referências explicativas em README/spec/plano são permitidas e devem ser inspecionadas manualmente.

- [ ] **Step 3: Auditar branch, worktree e commits**

```bash
git branch --show-current
git status --short --branch
git log --oneline origin/main..HEAD
git diff --check origin/main..HEAD
```

Expected: branch `main`, worktree limpo, somente commits do bootstrap e nenhum erro de diff.

- [ ] **Step 4: Encerrar containers de validação**

```bash
docker compose down --remove-orphans
```

- [ ] **Step 5: Push direto para main**

```bash
git push origin main
```

Expected: push aceito e `origin/main` atualizado.

- [ ] **Step 6: Verificar paridade remota**

```bash
git fetch origin main
git rev-parse HEAD
git rev-parse origin/main
git rev-list --left-right --count origin/main...HEAD
```

Expected: SHAs idênticos e contagem `0 0`.
