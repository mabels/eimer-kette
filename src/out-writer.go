package main

import (
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type Complete struct {
	completed bool
	todo      []types.Object
}

type OutWriter interface {
	setup() OutWriter
	write(items *[]types.Object)
	done()
}

func outWorker(app *S3StreamingLister, chstatus Queue) Queue {
	cho := makeChannelQueue(*app.config.maxKeys)
	var ow OutWriter
	if *app.config.format == "sqs" {
		ow = makeSqsOutWriter(app, chstatus)
	} else if *app.config.format == "mjson" {
		ow = makeMjsonOutWriter(app.output.fileStream)
	} else if *app.config.format == "awsls" {
		ow = makeAwsLsOutWriter(app.output.fileStream)
	} else if *app.config.format == "sqlite" {
		ow = makeSqliteOutWriter(app)
	}
	ow.setup()
	go (func() {
		cho.wait(func(items interface{}) {
			complete := items.(Complete)
			chstatus.push(RunStatus{outObjects: uint64(len(complete.todo))})
			ow.write(&complete.todo)
			if complete.completed {
				ow.done()
				// fmt.Fprintln(os.Stderr, "outWriter-Complete")
				chstatus.push(RunStatus{outObjects: 0, completed: true})
				cho.stop()
			}
		})
	})()
	return cho
}
