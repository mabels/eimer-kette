package main

import (
	// "go/types"
	"sync/atomic"
	// "context"
	// "encoding/json"
	// "fmt"
	// "io"
	// "math"
	// "os"
	// "sync/atomic"
	// "github.com/alitto/pond"
	// "github.com/aws/aws-lambda-go/events"
	// "github.com/aws/aws-sdk-go-v2/aws"
	// // "github.com/aws/aws-sdk-go-v2/config"
	// "github.com/aws/aws-sdk-go-v2/service/s3"
	// "github.com/aws/aws-sdk-go-v2/service/s3/types"
	// "github.com/aws/aws-sdk-go-v2/service/sqs"
)

func main() {
	app := defaultS3StreamingLister()
	initS3StreamingLister(app)

	chstatus := make(chan RunStatus, 100)

	cho := outWorker(app, chstatus)
	chi := s3ListerWorker(app, cho)

	if *app.config.strategie == "delimiter" {
		atomic.AddInt32(&app.inputConcurrent, 1)
		delimiterStrategie(&app.config, app.config.prefix, nil, chi)
	} else if *app.config.strategie == "letter" {
		atomic.AddInt32(&app.inputConcurrent, int32(len(*app.config.prefixes)))
		singleLetterStrategie(&app.config, app.config.prefix, chi)
	}

	statusWorker(app, chstatus)

	// fmt.Fprintln(os.Stderr, "Exit")
}
