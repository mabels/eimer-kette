package status

import (
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/mabels/s3-streaming-lister/config"
	myq "github.com/mabels/s3-streaming-lister/my-queue"
)

type RunStatus struct {
	Completed  bool
	Timed      bool
	OutObjects uint64
	Err        *error
}

type Complete struct {
	Completed bool
	Todo      []types.Object
}

func StatusWorker(app *config.S3StreamingLister, chstatus myq.MyQueue) {
	// fmt.Fprintf(os.Stderr, "statusWriter:0")
	total := uint64(0)
	lastTotal := uint64(0)
	abortTimer := false
	go func() {
		for !abortTimer {
			time.Sleep(5 * time.Second)
			chstatus.Push(RunStatus{Timed: true})
		}
	}()
	// fmt.Fprintf(os.Stderr, "statusWriter:1")
	chstatus.Wait(func(inItem interface{}) {
		// fmt.Fprintf(os.Stderr, "statusWriter:2")
		item := inItem.(RunStatus)
		if item.Err != nil {
			fmt.Fprintln(os.Stderr, *item.Err)
			return
		}
		total += item.OutObjects
		if item.Timed || item.Completed || lastTotal/(*app.Config.StatsFragment) != total/(*app.Config.StatsFragment) {
			if *app.Config.Progress {
				fmt.Fprintf(os.Stderr, "Done=%d inputConcurrent=%d listObjectsV2=%d/%d listObjectsV2Input=%d/%d NewFromConfig=%d/%d SqsSendMessage=%d/%d S3Deletes=%d/%d\n",
					total,
					app.InputConcurrent,
					app.Clients.Calls.Total.ListObjectsV2,
					app.Clients.Calls.Concurrent.ListObjectsV2,
					app.Clients.Calls.Total.ListObjectsV2Input,
					app.Clients.Calls.Concurrent.ListObjectsV2Input,
					app.Clients.Calls.Total.NewFromConfig,
					app.Clients.Calls.Concurrent.NewFromConfig,
					app.Clients.Calls.Total.SqsSendMessage,
					app.Clients.Calls.Concurrent.SqsSendMessage,
					app.Clients.Calls.Total.S3Deletes,
					app.Clients.Calls.Concurrent.S3Deletes,
				)
			}
			lastTotal = total
			if item.Completed {
				abortTimer = true
				chstatus.Stop()
				return
			}
		}
	})
}
