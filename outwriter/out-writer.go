package outwriter

import (
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/mabels/s3-streaming-lister/config"
	myq "github.com/mabels/s3-streaming-lister/my-queue"
	"github.com/mabels/s3-streaming-lister/status"
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
		cho.Wait(func(items interface{}) {
			complete := items.(status.Complete)
			chstatus.Push(status.RunStatus{OutObjects: uint64(len(complete.Todo))})
			ow.write(&complete.Todo)
			if complete.Completed {
				ow.done()
				// fmt.Fprintln(os.Stderr, "outWriter-Complete")
				chstatus.Push(status.RunStatus{OutObjects: 0, Completed: true})
				cho.Stop()
			}
		})
	})()
	return cho
}
