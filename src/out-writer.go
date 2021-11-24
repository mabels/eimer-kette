package main

import (
	"github.com/alitto/pond"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type Complete struct {
	completed bool
	todo      []types.Object
}

type OutWriter interface {
	setup() OutWriter
	write(tos types.Object)
	done()
}

func outWorker(app *S3StreamingLister, chstatus Queue) Queue {

	poolo := pond.New(*app.config.outWorkers, 0, pond.MinWorkers(*app.config.outWorkers))
	cho := makeChannelQueue(*app.config.maxKeys)
	var ow OutWriter
	if *app.config.format == "sqs" {
		ow = makeSqsOutWriter(app, poolo, chstatus)
	} else if *app.config.format == "mjson" {
		ow = makeMjsonOutWriter(app.output.fileStream)
	} else if *app.config.format == "awsls" {
		ow = makeAwsLsOutWriter(app.output.fileStream)
	}
	go (func() {
		cho.wait(func(items interface{}) {
			complete := items.(Complete)
			poolo.Submit(func() {
				chstatus.push(RunStatus{outObjects: uint64(len(complete.todo))})
				for _, item := range complete.todo {
					ow.write(item)
				}
				if complete.completed {
					ow.done()
					// fmt.Fprintln(os.Stderr, "outWriter-Complete")
					chstatus.push(RunStatus{outObjects: 0, completed: true})
					cho.abort()
				}
			})
		})
	})()
	return cho
}
