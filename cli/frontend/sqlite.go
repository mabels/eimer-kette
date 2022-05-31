package frontend

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/reactivex/rxgo/v2"

	config "github.com/mabels/eimer-kette/cli/config"
	myq "github.com/mabels/eimer-kette/cli/my-queue"
	"github.com/mabels/eimer-kette/cli/status"
)

func Sqlite(app *config.S3StreamingLister, cho myq.MyQueue, chStatus myq.MyQueue) {
	db, err := sql.Open("sqlite3", *app.Config.Frontend.Sqlite.Filename)
	if err != nil {
		chStatus.Push(status.RunStatus{Fatal: &err})
		return
	}
	var tableName string
	if *app.Config.Frontend.Sqlite.TableName != "" {
		tableName = *app.Config.Frontend.Sqlite.TableName
	} else {
		tableName = strings.ReplaceAll(strings.ReplaceAll(*app.Config.Bucket, ".", "_"), "-", "_")
	}
	go func() {
		producer := 0
		obs := rxgo.Defer([]rxgo.Producer{func(_ context.Context, ch chan<- rxgo.Item) {
			producer++
			// fmt.Fprintln(os.Stderr, "Producer:", producer)
			rows, err := db.Query(fmt.Sprintf(*app.Config.Frontend.Sqlite.Query, tableName))
			if err != nil {
				chStatus.Push(status.RunStatus{Fatal: &err})
			}
			defer rows.Close()
			cnt := 0
			for rows.Next() {
				cnt++
				var key string
				var time time.Time
				var size int64
				err := rows.Scan(&key, &time, &size)
				if err != nil {
					chStatus.Push(status.RunStatus{Err: &err})
				} else {
					res := types.Object{
						Key:          &key,
						LastModified: &time,
						Size:         size,
					} // with one element
					ch <- rxgo.Item{V: res}
				}
			}
			// fmt.Fprintln(os.Stderr, "pre-rows.close", cnt)
			// fmt.Fprintln(os.Stderr, "pre-ch-close", cnt)
			// close(ch)
			// fmt.Fprintln(os.Stderr, "post-ch-close", cnt)
		}}).BufferWithCount(1000)
		nextCnt := 0
		for item := range obs.Observe() {
			if item.E != nil {
				chStatus.Push(status.RunStatus{Err: &item.E})
			}
			if item.V != nil {
				items := item.V.([]interface{})
				nextCnt += len(items)
				// fmt.Fprintln(os.Stderr, "do-next", nextCnt)
				objs := make([]types.Object, 0, len(items))
				for _, item := range items {
					objs = append(objs, item.(types.Object))
				}
				app.Clients.Calls.Total.Inc("SqliteBuffer")
				// fmt.Fprintln(os.Stderr, "do-next-pre-post")
				cho.Push(Complete{Todo: objs, Completed: false})
				// fmt.Fprintln(os.Stderr, "do-next-post-post")
			}
		}
		// fmt.Fprintln(os.Stderr, "do-complete")
		cho.Push(Complete{Todo: nil, Completed: true})
		db.Close()
		// fmt.Fprintln(os.Stderr, "obs-run-done")
	}()
}
