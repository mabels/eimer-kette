BIN_NAME ?= "./s3-streaming-lister"
VERSION ?= dev
GITCOMMIT ?= $(shell git rev-list -1 HEAD)

all: test build

build:
	go build -ldflags "-s -w -X main.Version='$(VERSION)' -X main.GitCommit=$(GITCOMMIT)" -o $(BIN_NAME) s3-streaming-lister.adviser.com/src
	$(BIN_NAME) --version

test:
	go test s3-streaming-lister.adviser.com/src

