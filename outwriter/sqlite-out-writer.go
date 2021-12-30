package outwriter

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/reactivex/rxgo/v2"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/mabels/eimer-kette/config"
	myq "github.com/mabels/eimer-kette/my-queue"
	"github.com/mabels/eimer-kette/status"
)

type SqlLiteOutWriter struct {
	// pool       *pond.WorkerPool
	typesObjectChannel chan rxgo.Item
	app                *config.S3StreamingLister
	chStatus           myq.MyQueue
	waitComplete       sync.Mutex
	// db         *sql.DB
	// insertStmt *sql.Stmt
}

func (sow *SqlLiteOutWriter) setup() OutWriter {
	if *sow.app.Config.Output.Sqlite.CleanDb {
		os.Remove(*sow.app.Config.Output.Sqlite.Filename)
	}
	db, err := sql.Open("sqlite3", *sow.app.Config.Output.Sqlite.Filename)
	if err != nil {
		sow.chStatus.Push(status.RunStatus{Err: &err})
	}
	var tableName string
	if *sow.app.Config.Output.Sqlite.SqlTable != "" {
		tableName = *sow.app.Config.Output.Sqlite.SqlTable
	} else {
		tableName = strings.ReplaceAll(strings.ReplaceAll(*sow.app.Config.Bucket, ".", "_"), "-", "_")
	}
	// defer sow.db.Close()
	if *sow.app.Config.Output.Sqlite.CleanDb {
		createTable := fmt.Sprintf(`
		create table '%s' (
			key text,
			mtime date,
			size integer,
			created_at date);
	`, tableName)
		_, err = db.Exec(createTable)
		if err != nil {
			sow.chStatus.Push(status.RunStatus{Err: &err})
		}
	}
	sow.typesObjectChannel = make(chan rxgo.Item, *sow.app.Config.Output.Sqlite.CommitSize**sow.app.Config.Output.Sqlite.Workers)

	insertStmt, err := db.Prepare(
		fmt.Sprintf("insert into %s(key, mtime, size, created_at) values(?, ?, ?, CURRENT_TIMESTAMP)", tableName))
	if err != nil {
		sow.chStatus.Push(status.RunStatus{Err: &err})
	}

	observable := rxgo.FromChannel(sow.typesObjectChannel).BufferWithCount(*sow.app.Config.Output.Sqlite.CommitSize).Map(
		func(_ context.Context, items interface{}) (interface{}, error) {
			started := time.Now()
			tx, err := db.Begin()
			if err != nil {
				sow.chStatus.Push(status.RunStatus{Err: &err})
				return nil, err
			}
			my := tx.Stmt(insertStmt)
			for _, item := range items.([]interface{}) {
				obj := item.(types.Object)
				// fmt.Fprintf(os.Stderr, "%T %s %d", *obj.Key, obj.LastModified.String(), obj.Size)
				_, err := my.Exec(*obj.Key, *obj.LastModified, obj.Size)
				sow.app.Clients.Calls.Total.Inc("sqlite-insert")
				if err != nil {
					sow.app.Clients.Calls.Error.Inc("sqlite-insert")
					sow.chStatus.Push(status.RunStatus{Err: &err})
				}
			}

			//return sow.deleteObjects(item.([]interface{}))
			err = tx.Commit()
			sow.app.Clients.Calls.Total.Duration("sqlite-inserts", started)
			if err != nil {
				sow.chStatus.Push(status.RunStatus{Err: &err})
				return nil, err
			}
			return nil, nil
		},
		rxgo.WithPool(*sow.app.Config.Output.Sqs.Workers),
	)
	go func() {
		for range observable.Observe() {
		}
		sow.waitComplete.Unlock()
	}()
	return sow
}

func (sow *SqlLiteOutWriter) write(items *[]types.Object) {
	for _, obj := range *items {
		sow.typesObjectChannel <- rxgo.Item{V: obj}
	}
}

func (sow *SqlLiteOutWriter) done() {
	sow.waitComplete.Lock()
	close(sow.typesObjectChannel)
	sow.waitComplete.Unlock()
}

func makeSqliteOutWriter(app *config.S3StreamingLister, chStatus myq.MyQueue) OutWriter {
	if *app.Config.Output.Sqlite.Workers < 1 {
		panic("you need at least one worker for sqs")
	}
	// pool := pond.New(*app.Config.output.Sqlite.workers, 0, pond.MinWorkers(*app.Config.output.Sqlite.workers))
	// chunky, err := makeChunky(&events.S3Event{}, int(*app.Config.outputSqs.maxMessageSize))
	// if err != nil {
	// 	chStatus.Push(status.RunStatus{err: &err})
	// }
	sow := SqlLiteOutWriter{
		// chunky:     chunky,
		// chStatus:   chStatus,
		// pool: pool,
		// sqsClients: make(chan *sqs.Client, *app.Config.outputSqs.workers),
		chStatus: chStatus,
		app:      app,
		// waitComplete: sync.Mutex{},
	}
	sow.waitComplete.Lock()
	return &sow
}
