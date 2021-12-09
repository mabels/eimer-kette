package status

import (
	"fmt"
	"os"
	"sort"
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
	keysMaps := map[string]bool{}
	for _, call := range calls {
		for _, key := range call.Keys() {
			keysMaps[key] = true
		}
	}
	keys := []string{}
	for k, _ := range keysMaps {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := []string{}
	space := ""
	for _, key := range keys {
		out = append(out, space)
		space = " "
		items := []string{}
		items = append(items, fmt.Sprintf("%s=", key))
		slash := ""
		for _, call := range calls {
			items = append(items, fmt.Sprintf("%s%d", slash, call.Get(key).Cnt))
			slash = "/"
		}
		total := calls[0].Get(key)
		if total.Duration > 0 {
			items = append(items, fmt.Sprintf("%s%2.3f", slash, (float64(total.Duration)/float64(time.Second))/float64(total.Cnt)))
		}
		out = append(out, strings.Join(items, ""))

	}
	return strings.Join(out, " ")
}

func StatusWorker(app *config.S3StreamingLister, chstatus myq.MyQueue) {
	// fmt.Fprintf(os.Stderr, "statusWriter:0")
	total := uint64(0)
	// lastTotal := uint64(0)
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
		/* || lastTotal/(*app.Config.StatsFragment) != total/(*app.Config.StatsFragment) */
		if item.Timed || item.Completed {
			if *app.Config.Progress {
				fmt.Fprintf(os.Stderr, "Done=%d inputConcurrent=%d %s\n",
					total,
					app.InputConcurrent,
					statString(&app.Clients.Calls.Total, &app.Clients.Calls.Concurrent, &app.Clients.Calls.Error))
			}
			// lastTotal = total
			if item.Completed {
				abortTimer = true
				chstatus.Stop()
				return
			}
		}
	})
}
