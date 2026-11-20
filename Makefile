SHELL := /bin/bash
GO    ?= go
DB_URL ?= postgres://notifications:notifications@localhost:5432/notifications?sslmode=disable

.PHONY: help build up down logs test test-unit lint migrate seed fmt

help: ## Show available targets
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-18s %s\n", $$1, $$2}'

build: ## Compile all binaries into ./bin
	$(GO) build -o bin/ ./cmd/...

up: ## Start the full stack (postgres, redis, migrate, api, worker, mockprovider, swagger-ui)
	docker compose up --build -d
	@echo "API:        http://localhost:8080"
	@echo "Swagger UI: http://localhost:8081"
	@echo "Worker:     http://localhost:8082/health/ready"

down: ## Stop the stack and delete volumes
	docker compose down -v

logs: ## Tail api and worker logs
	docker compose logs -f api worker

test: ## Run unit and integration tests (integration needs Docker)
	$(GO) test -race -count=1 -tags=integration ./...

test-unit: ## Run fast unit tests only (no Docker)
	$(GO) test -race -count=1 ./...

lint: ## Run go vet and golangci-lint
	$(GO) vet ./...
	golangci-lint run --build-tags=integration ./...

migrate: ## Apply migrations to DB_URL using the migrate container
	docker run --rm --network host -v $(PWD)/migrations:/migrations:ro \
		migrate/migrate:v4.20.1 -path=/migrations -database "$(DB_URL)" up

seed: ## Send a demo batch (all channels/priorities, plus forced failures)
	./scripts/seed.sh

fmt: ## Format code
	gofmt -w .
