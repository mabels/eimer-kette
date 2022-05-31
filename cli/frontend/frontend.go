package frontend

import (
	"sync/atomic"

	config "github.com/mabels/eimer-kette/cli/config"
	myq "github.com/mabels/eimer-kette/cli/my-queue"
)

func Frontend(app *config.S3StreamingLister, cho myq.MyQueue, chstatus myq.MyQueue) {
	if *app.Config.Frontend.Frontend == "parquet" {
		Parquet(app, cho, chstatus)
	} else if *app.Config.Frontend.Frontend == "sqlite" {
		Sqlite(app, cho, chstatus)
	} else if *app.Config.Frontend.Frontend == "aws-s3" {
		chi := S3ListerWorker(app, cho, chstatus)
		if *app.Config.Strategy == "delimiter" {
			atomic.AddInt32(&app.InputConcurrent, 1)
			DelimiterStrategy(app, app.Config.Prefix, nil, chi)
		} else if *app.Config.Strategy == "letter" {
			atomic.AddInt32(&app.InputConcurrent, int32(len(*app.Config.Prefixes)))
			SingleLetterStrategy(app, app.Config.Prefix, chi)
		}
	} else if *app.Config.Frontend.Frontend == "aws-lambda-lister" {
		AwsLambdaLister(app, cho, chstatus)
	}
}
