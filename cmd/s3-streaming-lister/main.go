package main

import (
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/mabels/s3-streaming-lister/config"
	"github.com/mabels/s3-streaming-lister/frontend"
	myq "github.com/mabels/s3-streaming-lister/my-queue"
	ow "github.com/mabels/s3-streaming-lister/outwriter"
	"github.com/mabels/s3-streaming-lister/status"
)

var version = "develop"
var commit = "unknown"

func main() {
	config.Version = version
	config.GitCommit = commit
	app := config.DefaultS3StreamingLister()
	config.InitS3StreamingLister(app)
	if *app.Config.Lambda.Start {
		lambda.Start(AwsHandleRequest)
		return
	}

	chstatus := myq.MakeChannelQueue(*app.Config.Output.Sqs.Workers * *app.Config.S3Workers * 10)
	cho := ow.OutWriterProcessor(app, chstatus)
	frontend.Frontend(app, cho, chstatus)

	status.StatusWorker(app, chstatus)
}
