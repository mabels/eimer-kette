
all: ../handler/lambda.zip main.go
	PULUMI_CONFIG_PASSPHRASE="" pulumi up -s eimer-fperson1 -f

../handler/lambda.zip: ../handler/main
	cd ../handler && zip -o lambda.zip main

../handler/main: ../handler/main.go Makefile ../handler/write-files.go
	cd ../handler && GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o main ./...
