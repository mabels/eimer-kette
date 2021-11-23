package main

import (
	"fmt"
	"os"
)

type RunStatus struct {
	completed  bool
	outObjects uint64
	err        *error
}

func statusWorker(app *S3StreamingLister, chstatus chan RunStatus) {
	total := uint64(0)
	lastTotal := uint64(0)
	for item := range chstatus {
		if item.err != nil {
			fmt.Fprintln(os.Stderr, *item.err)
			continue
		}
		total += item.outObjects
		if item.completed || lastTotal/(*app.config.statsFragment) != total/(*app.config.statsFragment) {
			if *app.config.progress {
				fmt.Fprintf(os.Stderr, "Done=%d inputConcurrent=%d listObjectsV2=%d/%d listObjectsV2Input=%d/%d NewFromConfig=%d/%d\n",
					total,
					app.inputConcurrent,
					app.clients.calls.total.listObjectsV2,
					app.clients.calls.concurrent.listObjectsV2,
					app.clients.calls.total.listObjectsV2Input,
					app.clients.calls.concurrent.listObjectsV2Input,
					app.clients.calls.total.newFromConfig,
					app.clients.calls.concurrent.newFromConfig,
				)
			}
			lastTotal = total
			if item.completed {
				break
			}
		}
	}
}
