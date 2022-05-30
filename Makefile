all: test build

build:
	goreleaser build --single-target --skip-validate --rm-dist

test:
	go test github.com/mabels/eimer-kette/my-queue
	go test github.com/mabels/eimer-kette/timed-worker

