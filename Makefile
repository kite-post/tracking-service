# Той самий набір цілей, що й у решти репозиторіїв Kite: що в Java, що в Go,
# що у Vue — `make lint`, `make test`, `make image` роблять очікуване.

SERVICE ?= $(notdir $(CURDIR))
ORG     ?= kite-post
IMAGE   ?= ghcr.io/$(ORG)/$(SERVICE)
TAG     ?= dev
MVN     := $(if $(wildcard ./mvnw),./mvnw,mvn)
# База сервіса на стенді: shipment-service -> shipment (kite-infra, config/postgres/init).
DB      ?= $(subst -,,$(patsubst %-service,%,$(SERVICE)))
# Мережа стенда kite-infra (name: kite у його docker-compose.yml).
STAND_NETWORK ?= kite_default

# Тег kite-contracts, за яким закріплений сервіс. Той самий номер лежить у
# pom.xml властивістю kite-contracts.version; contracts-check стежить, щоб
# вони не розійшлися.
CONTRACTS_VERSION ?= 0.2.1

.DEFAULT_GOAL := help
.PHONY: help build lint fmt test test-it verify image run run-stand up down clean contracts contracts-check rename

help: ## Показати цілі
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

build: ## Зібрати jar без тестів
	$(MVN) -B -ntp clean package -DskipTests

lint: ## Перевірити формат і імпорти
	$(MVN) -B -ntp spotless:check

fmt: ## Відформатувати код
	$(MVN) -B -ntp spotless:apply

test: ## Юніт-тести
	$(MVN) -B -ntp test

test-it: ## Інтеграційні тести (*IT) разом з юніт-
	$(MVN) -B -ntp verify

verify: lint test-it ## Усе, що робить CI

image: ## Зібрати образ локально
	docker build -t $(IMAGE):$(TAG) .

run: ## Запустити локально: Postgres і Kafka піднімуться контейнерами самі
	$(MVN) -B -ntp spring-boot:test-run

run-stand: ## Запустити локально проти стенда kite-infra (localhost:5432, :9092)
	$(MVN) -B -ntp spring-boot:run \
		-Dspring-boot.run.arguments="--spring.datasource.url=jdbc:postgresql://localhost:5432/$(DB) --spring.datasource.username=$(DB)"

up: image ## Підняти образ на 8080 у мережі стенда kite-infra (стенд має бути запущений)
	docker run --rm -p 8080:8080 --memory 384m --name $(SERVICE) --network $(STAND_NETWORK) \
		-e SPRING_DATASOURCE_URL=jdbc:postgresql://postgres:5432/$(DB) \
		-e SPRING_DATASOURCE_USERNAME=$(DB) \
		-e SPRING_DATASOURCE_PASSWORD=kite \
		-e SPRING_KAFKA_BOOTSTRAP_SERVERS=kafka:19092 \
		$(IMAGE):$(TAG)

down: ## Зупинити контейнер
	-docker rm -f $(SERVICE)

clean: ## Прибрати артефакти збірки
	$(MVN) -B -ntp clean

contracts: ## Підключити kite-contracts за тегом; нова версія — make contracts V=0.2.0
	@bash scripts/contracts.sh "$(if $(V),$(V),$(CONTRACTS_VERSION))"

contracts-check: ## Перевірити, що контракти підключені за тегом і копій схем немає
	@bash scripts/contracts-check.sh "$(CONTRACTS_VERSION)"

rename: ## Перейменувати шаблон у справжній сервіс: make rename NAME=shipment-service
	@test -n "$(NAME)" || (echo "Вкажи ім'я: make rename NAME=shipment-service"; exit 1)
	@bash scripts/rename.sh "$(NAME)"
