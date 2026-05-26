APP_NAME := proxy-acp-codex
BIN_DIR := bin
DIST_DIR := dist
ENV ?= .env

.PHONY: build build-windows-amd64 run test fmt clean

build:
	mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/$(APP_NAME) ./cmd/proxy-acp-codex

build-windows-amd64:
	mkdir -p $(DIST_DIR)/windows-amd64
	GOOS=windows GOARCH=amd64 go build -o $(DIST_DIR)/windows-amd64/$(APP_NAME).exe ./cmd/proxy-acp-codex

run:
	go run ./cmd/proxy-acp-codex -env $(ENV)

test:
	go test ./...

fmt:
	gofmt -w cmd internal

clean:
	rm -rf $(BIN_DIR) $(DIST_DIR)
