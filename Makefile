all: test build

build:
	goreleaser build --single-target --skip-validate --rm-dist

test:
	go test github.com/mabels/s3-streaming-lister/my-queue

