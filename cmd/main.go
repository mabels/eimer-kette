package main

import (
	"sync/atomic"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/mabels/s3-streaming-lister/config"
	"github.com/mabels/s3-streaming-lister/frontend"
	myq "github.com/mabels/s3-streaming-lister/my-queue"
	ow "github.com/mabels/s3-streaming-lister/outwriter"
	"github.com/mabels/s3-streaming-lister/status"
)

func main() {
	app := config.DefaultS3StreamingLister()
	config.InitS3StreamingLister(app)
	if *app.Config.Lambda.Start {
		lambda.Start(AwsHandleRequest)
		return
	}

	chstatus := myq.MakeChannelQueue(*app.Config.Output.Sqs.Workers * *app.Config.S3Workers * 10)
	cho := ow.OutWriterProcessor(app, chstatus)
	chi := frontend.S3ListerWorker(app, cho, chstatus)

	if *app.Config.Strategy == "delimiter" {
		atomic.AddInt32(&app.InputConcurrent, 1)
		frontend.DelimiterStrategy(app, app.Config.Prefix, nil, chi)
	} else if *app.Config.Strategy == "letter" {
		atomic.AddInt32(&app.InputConcurrent, int32(len(*app.Config.Prefixes)))
		frontend.SingleLetterStrategy(app, app.Config.Prefix, chi)
	}
	status.StatusWorker(app, chstatus)
}
