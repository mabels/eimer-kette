BIN_NAME ?= "./s3-streaming-lister"
VERSION ?= dev
GITCOMMIT ?= $(shell git rev-list -1 HEAD)

all: test build

build:
	go build -ldflags "-s -w -X main.Version='$(VERSION)' -X main.GitCommit=$(GITCOMMIT)" -o $(BIN_NAME) github.com/mabels/s3-streaming-lister/cmd
	$(BIN_NAME) --version

test:
	go test github.com/mabels/s3-streaming-lister/my-queue

