package outwriter

import (
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/mabels/eimer-kette/config"
	"github.com/mabels/eimer-kette/frontend"
	myq "github.com/mabels/eimer-kette/my-queue"
	"github.com/mabels/eimer-kette/status"
)

type OutWriter interface {
	setup() OutWriter
	write(items *[]types.Object)
	done()
}

func OutWriterProcessor(app *config.S3StreamingLister, chstatus myq.MyQueue) myq.MyQueue {
	cho := myq.MakeChannelQueue(*app.Config.MaxKeys)
	var ow OutWriter
	if *app.Config.Format == "sqs" {
		ow = makeSqsOutWriter(app, chstatus)
	} else if *app.Config.Format == "mjson" {
		ow = makeMjsonOutWriter(app.Output.FileStream)
	} else if *app.Config.Format == "awsls" {
		ow = makeAwsLsOutWriter(app.Output.FileStream)
	} else if *app.Config.Format == "sqlite" {
		ow = makeSqliteOutWriter(app, chstatus)
	} else if *app.Config.Format == "dynamo" {
		ow = makeDynamoOutWriter(app)
	} else if *app.Config.Format == "s3delete" {
		ow = makeS3DeleteOutWriter(app, chstatus)
	}
	ow.setup()
	go (func() {
		todos := 0
		cho.Wait(func(items interface{}) {
			complete := items.(frontend.Complete)
			chstatus.Push(status.RunStatus{OutObjects: uint64(len(complete.Todo))})
			todos += len(complete.Todo)
			// fmt.Fprintln(os.Stderr, "ow.write:", todos)
			ow.write(&complete.Todo)
			if complete.Completed {
				fmt.Fprintln(os.Stderr, "outWriter-Complete-pre")
				ow.done()
				fmt.Fprintln(os.Stderr, "outWriter-Complete-post")
				chstatus.Push(status.RunStatus{OutObjects: 0, Completed: true})
				cho.Stop()
			}
		})
	})()
	return cho
}
