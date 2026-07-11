.PHONY: help setup up down kafka-up obs-up migrate run run-kafka run-ingest run-worker build docker loadtest chaos test fmt

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-11s\033[0m %s\n", $$1, $$2}'

setup: ## Download Go dependencies
	go mod tidy

up: ## Start Postgres + Redis (Docker), waiting until healthy
	docker compose up -d --wait

kafka-up: ## Start Postgres + Redis + Redpanda (durable queue)
	docker compose --profile kafka up -d

obs-up: ## Start Prometheus + Grafana (metrics stack)
	docker compose --profile obs up -d
	@echo "Prometheus: http://localhost:9090   Grafana: http://localhost:3000"

down: ## Stop and remove all containers
	docker compose --profile kafka --profile obs down

migrate: ## Apply all database migrations (idempotent)
	@echo "waiting for postgres to be ready..."
	@until docker compose exec -T postgres psql -U tally -d tally -c 'SELECT 1' >/dev/null 2>&1; do sleep 1; done
	@for f in migrations/*.sql; do \
		echo "applying $$f"; \
		docker compose exec -T postgres psql -U tally -d tally -v ON_ERROR_STOP=1 < $$f; \
	done

run: ## Run Tally (in-memory queue)
	go run ./cmd/tally

run-kafka: ## Run Tally with the durable queue (needs kafka-up)
	QUEUE=kafka go run ./cmd/tally

run-ingest: ## Run an ingest-only instance on :8080 (needs kafka-up)
	QUEUE=kafka MODE=ingest go run ./cmd/tally

run-worker: ## Run a worker-only instance on :8081 (needs kafka-up)
	QUEUE=kafka MODE=worker ADDR=:8081 go run ./cmd/tally

build: ## Build binaries into ./bin
	go build -o bin/tally ./cmd/tally
	go build -o bin/loadgen ./cmd/loadgen

docker: ## Build the Docker image
	docker build -t tally:local .

loadtest: ## Fire fake traffic with the Go generator (see also loadtest/ingest.js for k6)
	go run ./cmd/loadgen -rate 2000 -duration 10s

chaos: ## Kill a worker mid-batch and prove nothing is lost (needs kafka-up + build)
	./scripts/chaos.sh

test: ## Run tests with the race detector
	go test -race ./...

fmt: ## Format code
	go fmt ./...
