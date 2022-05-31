package main

import (
	"github.com/mabels/eimer-kette/cli/config"
	"github.com/mabels/eimer-kette/cli/frontend"
	myq "github.com/mabels/eimer-kette/cli/my-queue"
	ow "github.com/mabels/eimer-kette/cli/outwriter"
	"github.com/mabels/eimer-kette/cli/status"
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
	statusStartedDone := make(chan bool)
	go func() {
		statusStartedDone <- true
		status.StatusWorker(app, chstatus)
		statusStartedDone <- true
	}()
	<-statusStartedDone

	cho := ow.OutWriterProcessor(app, chstatus)
	frontend.Frontend(app, cho, chstatus)

	<-statusStartedDone
}
