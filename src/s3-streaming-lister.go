package main

import (
	"sync/atomic"
)

func main() {
	app := defaultS3StreamingLister()
	initS3StreamingLister(app)

	chstatus := makeChannelQueue(*app.config.outputSqs.workers * *app.config.s3Workers * 10)
	cho := outWorker(app, chstatus)
	chi := s3ListerWorker(app, cho, chstatus)

	if *app.config.strategie == "delimiter" {
		atomic.AddInt32(&app.inputConcurrent, 1)
		delimiterStrategie(&app.config, app.config.prefix, nil, chi)
	} else if *app.config.strategie == "letter" {
		atomic.AddInt32(&app.inputConcurrent, int32(len(*app.config.prefixes)))
		singleLetterStrategie(&app.config, app.config.prefix, chi)
	}
	statusWorker(app, chstatus)
}
