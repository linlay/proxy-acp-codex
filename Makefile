APP_NAME := proxy-acp-codex
BIN_DIR := bin
DIST_DIR := dist
RELEASE_DIR := $(DIST_DIR)/release
VERSION := v0.1.0
OS := darwin
ARCH := arm64
ASSET_NAME := $(APP_NAME)-$(VERSION)-$(OS)-$(ARCH).tar.gz
ENV ?= .env

.PHONY: build build-windows-amd64 release-plugin release-program run test fmt clean

build:
	mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/$(APP_NAME) ./cmd/proxy-acp-codex

build-windows-amd64:
	mkdir -p $(DIST_DIR)/windows-amd64
	GOOS=windows GOARCH=amd64 go build -o $(DIST_DIR)/windows-amd64/$(APP_NAME).exe ./cmd/proxy-acp-codex

release-program: build
	rm -rf $(DIST_DIR)/bundle
	mkdir -p $(DIST_DIR)/bundle/$(APP_NAME)/backend \
		$(DIST_DIR)/bundle/$(APP_NAME)/scripts \
		$(RELEASE_DIR)
	cp $(BIN_DIR)/$(APP_NAME) $(DIST_DIR)/bundle/$(APP_NAME)/backend/$(APP_NAME)
	cp manifest.json .env.example start.sh stop.sh deploy.sh $(DIST_DIR)/bundle/$(APP_NAME)/
	cp scripts/program-common.sh $(DIST_DIR)/bundle/$(APP_NAME)/scripts/program-common.sh
	chmod +x $(DIST_DIR)/bundle/$(APP_NAME)/backend/$(APP_NAME) \
		$(DIST_DIR)/bundle/$(APP_NAME)/start.sh \
		$(DIST_DIR)/bundle/$(APP_NAME)/stop.sh \
		$(DIST_DIR)/bundle/$(APP_NAME)/deploy.sh \
		$(DIST_DIR)/bundle/$(APP_NAME)/scripts/program-common.sh
	tar -czf $(RELEASE_DIR)/$(ASSET_NAME) -C $(DIST_DIR)/bundle $(APP_NAME)

release-plugin: release-program

run:
	go run ./cmd/proxy-acp-codex -env $(ENV)

test:
	go test ./...

fmt:
	gofmt -w cmd internal

clean:
	rm -rf $(BIN_DIR) $(DIST_DIR)
