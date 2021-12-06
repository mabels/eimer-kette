package frontend

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	config "github.com/mabels/s3-streaming-lister/config"
	myq "github.com/mabels/s3-streaming-lister/my-queue"
	"github.com/mabels/s3-streaming-lister/status"
)

func Sqlite(app *config.S3StreamingLister, cho myq.MyQueue, chStatus myq.MyQueue) {

	db, err := sql.Open("sqlite3", *app.Config.Frontend.Sqlite.Filename)
	if err != nil {
		chStatus.Push(status.RunStatus{Err: &err})
	}
	var tableName string
	if *app.Config.Frontend.Sqlite.TableName != "" {
		tableName = *app.Config.Frontend.Sqlite.TableName
	} else {
		tableName = strings.ReplaceAll(strings.ReplaceAll(*app.Config.Bucket, ".", "_"), "-", "_")
	}
	go func() {
		rows, err := db.Query(fmt.Sprintf(*app.Config.Frontend.Sqlite.Query, tableName))
		if err != nil {
			chStatus.Push(status.RunStatus{Err: &err})
		}
		defer rows.Close()

		resArray := []types.Object{{}} // with one element
		res := &resArray[0]
		for rows.Next() {
			var key string
			var time time.Time
			err := rows.Scan(&key, &time, &res.Size)
			if err != nil {
				chStatus.Push(status.RunStatus{Err: &err})
			} else {
				res.Key = &key
				res.LastModified = &time
				cho.Push(status.Complete{Todo: resArray, Completed: false})
			}
		}
		cho.Push(status.Complete{Todo: nil, Completed: true})
	}()
}
