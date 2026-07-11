.PHONY: help setup up down migrate run build loadtest test fmt

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

setup: ## Download Go dependencies
	go mod tidy

up: ## Start Postgres + Redis (Docker)
	docker compose up -d

down: ## Stop and remove containers
	docker compose down

migrate: ## Apply all database migrations (idempotent)
	@for f in migrations/*.sql; do \
		echo "applying $$f"; \
		docker compose exec -T postgres psql -U tally -d tally -v ON_ERROR_STOP=1 < $$f; \
	done

run: ## Run the Tally service
	go run ./cmd/tally

build: ## Build binaries into ./bin
	go build -o bin/tally ./cmd/tally
	go build -o bin/loadgen ./cmd/loadgen

loadtest: ## Fire fake traffic with the Go generator
	go run ./cmd/loadgen -rate 2000 -duration 10s

test: ## Run tests
	go test -race ./...

fmt: ## Format code
	go fmt ./...
