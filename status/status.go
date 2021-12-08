package status

import (
	"fmt"
	"os"
	"strings"
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

func statString(calls ...*config.Calls) string {
	keys := map[string]bool{}
	for _, call := range calls {
		for _, key := range call.Keys() {
			keys[key] = true
		}
	}
	out := []string{}
	space := ""
	for key := range keys {
		out = append(out, space)
		space = " "
		out = append(out, fmt.Sprintf("%s=", key))
		slash := ""
		for _, call := range calls {
			out = append(out, fmt.Sprintf("%s%d", slash, call.Get(key).Cnt))
			slash = "/"
		}
		total := calls[0].Get(key)
		out = append(out, fmt.Sprintf("%s%2.3f", slash, (float64(total.Duration)/float64(time.Microsecond))/float64(total.Cnt)))

	}
	return strings.Join(out, " ")
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
				fmt.Fprintf(os.Stderr, "Done=%d inputConcurrent=%d %s\n",
					total,
					app.InputConcurrent,
					statString(&app.Clients.Calls.Total, &app.Clients.Calls.Concurrent, &app.Clients.Calls.Error))
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
