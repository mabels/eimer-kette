package eimerkette

import (
	"github.com/mabels/eimer-kette/config"
	"github.com/mabels/eimer-kette/frontend"
	myq "github.com/mabels/eimer-kette/my-queue"
	ow "github.com/mabels/eimer-kette/outwriter"
	"github.com/mabels/eimer-kette/status"
)

var version = "develop"
var commit = "unknown"

func CliMain() {

	config.Version = version
	config.GitCommit = commit
	app := config.DefaultS3StreamingLister()
	config.InitS3StreamingLister(app)

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
