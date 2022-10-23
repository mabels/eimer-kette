package status

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/mabels/eimer-kette/config"
	myq "github.com/mabels/eimer-kette/my-queue"
)

type RunStatus struct {
	Completed  bool
	Timed      bool
	OutObjects uint64
	Err        error
	Fatal      error
}

type RunResult struct {
	Action  string
	Took    time.Duration
	Related interface{}
	Info    *string
	Warning *string
	Err     error
	Fatal   error
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

func StatusWorker(app *config.EimerKette, chstatus myq.MyQueue) {
	// fmt.Fprintf(os.Stderr, "statusWriter:0")
	total := uint64(0)
	// lastTotal := uint64(0)
	abortTimer := false
	if *app.Config.Progress > 0 {
		go func() {
			for !abortTimer {
				time.Sleep(time.Duration(*app.Config.Progress) * time.Second)
				chstatus.Push(RunStatus{Timed: true})
			}
		}()
	}
	took := time.Now()
	// fmt.Fprintf(os.Stderr, "statusWriter:1")
	chstatus.Wait(func(inItem interface{}) {
		// fmt.Fprintf(os.Stderr, "statusWriter:0:%v\n", inItem)
		item := inItem.(RunStatus)
		if item.Err != nil {
			// fmt.Fprintf(os.Stderr, "statusWriter:2\n")
			fmt.Fprintln(os.Stderr, item.Err)
			return
		}
		if item.Fatal != nil {
			// fmt.Fprintf(os.Stderr, "statusWriter:2\n")
			fmt.Fprintln(os.Stderr, item.Fatal)
			item.Completed = true
		}
		total += item.OutObjects
		/* || lastTotal/(*app.Config.StatsFragment) != total/(*app.Config.StatsFragment) */
		if item.Timed || item.Completed {
			fmt.Fprintf(os.Stderr, "Now=%s Total=%d/%d %s\n",
				time.Now().Format(time.RFC3339),
				total,
				app.InputConcurrent,
				statString(&app.Clients.Calls.Total, &app.Clients.Calls.Concurrent, &app.Clients.Calls.Error))
			// lastTotal = total
			if item.Completed {
				since := time.Since(took)
				fmt.Fprintf(os.Stderr, "Now=%s Took=%d:%02d:%02d",
					time.Now().Format(time.RFC3339),
					int(since.Hours()), int(since.Minutes()), int(since.Seconds()))
				abortTimer = true
				chstatus.Stop()
				return
			}
		}
	})
}
