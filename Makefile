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
