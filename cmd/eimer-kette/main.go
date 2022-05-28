package main

import (
	"github.com/mabels/eimer-kette/config"
	"github.com/mabels/eimer-kette/frontend"
	myq "github.com/mabels/eimer-kette/my-queue"
	ow "github.com/mabels/eimer-kette/outwriter"
	"github.com/mabels/eimer-kette/status"
)

var version = "develop"
var commit = "unknown"

func main() {
	config.Version = version
	config.GitCommit = commit
	app := config.DefaultS3StreamingLister()
	config.InitS3StreamingLister(app)
	if *app.Config.Lambda.Start {
		// lambda.Start(AwsHandleRequest)
		return
	}

	chstatus := myq.MakeChannelQueue(*app.Config.Output.Sqs.Workers * *app.Config.S3Workers * 10)
	cho := ow.OutWriterProcessor(app, chstatus)
	frontend.Frontend(app, cho, chstatus)

	status.StatusWorker(app, chstatus)
}
