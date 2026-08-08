# Beacon — sharded real-time presence and session-join service.
#
# Every target here runs against localhost only. Nothing in this Makefile
# contacts a registry, a cloud provider, or a code-hosting service.

SHELL       := /bin/sh
BINARY      := bin/gateway
PKG         := ./...
COMPOSE     := docker compose -f deploy/docker-compose.yml
LOAD_VUS    ?= 10000

.DEFAULT_GOAL := help

## help: list available targets
help:
	@echo "Beacon targets:"
	@grep -E '^## ' $(MAKEFILE_LIST) | sed 's/## /  /'

## build: compile the gateway binary
build:
	go build -o $(BINARY) ./cmd/gateway

## test: run all unit tests
test:
	go test $(PKG)

## test-race: run all unit tests under the race detector
test-race:
	go test -race $(PKG)

## integration: cross-node tests against a running stack (needs `make up` first)
integration:
	go test -tags=integration -count=1 -v ./test/...

## vet: run go vet across the module
vet:
	go vet $(PKG)

## fmt: format all Go source
fmt:
	go fmt $(PKG)

## tidy: reconcile go.mod / go.sum
tidy:
	go mod tidy

## check: fmt + vet + test, the pre-commit gate
check: fmt vet test

## up: start the full stack (3 gateways, Redis, Prometheus, Grafana)
up:
	$(COMPOSE) up -d --build

## down: stop the stack and remove volumes
down:
	$(COMPOSE) down -v

## logs: tail logs from every service
logs:
	$(COMPOSE) logs -f

## ps: show stack status
ps:
	$(COMPOSE) ps

## load: run the k6 load test (override with LOAD_VUS=3000)
load:
	$(COMPOSE) run --rm -e LOAD_VUS=$(LOAD_VUS) k6 run /scripts/load.js

## chaos: run the gateway-kill chaos measurement
chaos:
	bash bench/chaos.sh

## clean: remove build output
clean:
	rm -rf bin/

.PHONY: help build test test-race integration vet fmt tidy check up down logs ps load chaos clean
