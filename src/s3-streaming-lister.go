package main

import (
	"sync/atomic"

	"github.com/aws/aws-lambda-go/lambda"
)

func main() {
	app := defaultS3StreamingLister()
	initS3StreamingLister(app)
	if *app.config.lambda.start {
		lambda.Start(AwsHandleRequest)
		return
	}

	chstatus := makeChannelQueue(*app.config.output.Sqs.workers * *app.config.s3Workers * 10)
	cho := outWorker(app, chstatus)
	chi := s3ListerWorker(app, cho, chstatus)

	if *app.config.strategy == "delimiter" {
		atomic.AddInt32(&app.inputConcurrent, 1)
		delimiterStrategy(app, app.config.prefix, nil, chi)
	} else if *app.config.strategy == "letter" {
		atomic.AddInt32(&app.inputConcurrent, int32(len(*app.config.prefixes)))
		singleLetterStrategy(app, app.config.prefix, chi)
	}
	statusWorker(app, chstatus)
}
