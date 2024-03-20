SHELL=/bin/bash

.PHONY: all
all: deps lint test

.PHONY: deps
deps:
	go mod download

.PHONY: test
test:
	go test -race -count=1 ./...

.PHONY: lint
lint:
	golangci-lint run --skip-dirs ./wire
	staticcheck ./...

.PHONY: install
install:
	go install honnef.co/go/tools/cmd/staticcheck@latest
