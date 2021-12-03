package main

import (
	"fmt"
	"os"
	"time"
)

type RunStatus struct {
	completed  bool
	timed      bool
	outObjects uint64
	err        *error
}

func statusWorker(app *S3StreamingLister, chstatus Queue) {
	// fmt.Fprintf(os.Stderr, "statusWriter:0")
	total := uint64(0)
	lastTotal := uint64(0)
	abortTimer := false
	go func() {
		for !abortTimer {
			time.Sleep(5 * time.Second)
			chstatus.push(RunStatus{timed: true})
		}
	}()
	// fmt.Fprintf(os.Stderr, "statusWriter:1")
	chstatus.wait(func(inItem interface{}) {
		// fmt.Fprintf(os.Stderr, "statusWriter:2")
		item := inItem.(RunStatus)
		if item.err != nil {
			fmt.Fprintln(os.Stderr, *item.err)
			return
		}
		total += item.outObjects
		if item.timed || item.completed || lastTotal/(*app.config.statsFragment) != total/(*app.config.statsFragment) {
			if *app.config.progress {
				fmt.Fprintf(os.Stderr, "Done=%d inputConcurrent=%d listObjectsV2=%d/%d listObjectsV2Input=%d/%d NewFromConfig=%d/%d SqsSendMessage=%d/%d\n",
					total,
					app.inputConcurrent,
					app.clients.calls.total.listObjectsV2,
					app.clients.calls.concurrent.listObjectsV2,
					app.clients.calls.total.listObjectsV2Input,
					app.clients.calls.concurrent.listObjectsV2Input,
					app.clients.calls.total.newFromConfig,
					app.clients.calls.concurrent.newFromConfig,
					app.clients.calls.total.sqsSendMessage,
					app.clients.calls.concurrent.sqsSendMessage,
				)
			}
			lastTotal = total
			if item.completed {
				abortTimer = true
				chstatus.stop()
				return
			}
		}
	})
}
