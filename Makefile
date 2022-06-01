GOS=$(find . -name "*.go" -print)

all: test dist/cli dist/amd64-lambda.zip

dist/cli: dist 
	go build -o dist/cli main.go

pulumi: dist/amd64-lambda.zip
	PULUMI_CONFIG_PASSPHRASE="" pulumi up -s eimer-fperson1 -f

release:
	goreleaser build --single-target --skip-validate --rm-dist

dist/amd64-lambda.zip: dist/amd64-lambda
	cd dist && zip -o amd64-lambda.zip amd64-lambda

dist/amd64-lambda: dist $(GOS)
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o dist/amd64-lambda main.go

dist:
	mkdir -p dist

clean:
	rm -rf ./dist

test:
	go test ./...

