package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	_ "github.com/mattn/go-sqlite3"
	"github.com/reactivex/rxgo/v2"
)

type SqlLiteOutWriter struct {
	// pool       *pond.WorkerPool
	typesObjectChannel chan rxgo.Item
	app                *S3StreamingLister
	chStatus           MyQueue
	waitComplete       sync.Mutex
	// db         *sql.DB
	// insertStmt *sql.Stmt
}

func (sow *SqlLiteOutWriter) setup() OutWriter {
	if *sow.app.config.output.Sqlite.cleanDb {
		os.Remove(*sow.app.config.output.Sqlite.filename)
	}
	db, err := sql.Open("sqlite3", *sow.app.config.output.Sqlite.filename)
	if err != nil {
		sow.chStatus.push(RunStatus{err: &err})
	}
	var tableName string
	if *sow.app.config.output.Sqlite.sqlTable != "" {
		tableName = *sow.app.config.output.Sqlite.sqlTable
	} else {
		tableName = strings.ReplaceAll(strings.ReplaceAll(*sow.app.config.bucket, ".", "_"), "-", "_")

	}
	// defer sow.db.Close()
	if *sow.app.config.output.Sqlite.cleanDb {
		createTable := fmt.Sprintf(`
		create table '%s' (
			key text,
			mtime date,
			size integer);
	`, tableName)
		_, err = db.Exec(createTable)
		if err != nil {
			sow.chStatus.push(RunStatus{err: &err})
		}
	}
	sow.typesObjectChannel = make(chan rxgo.Item, *sow.app.config.output.Sqlite.commitSize**sow.app.config.output.Sqlite.workers)

	insertStmt, err := db.Prepare(
		fmt.Sprintf("insert into %s(key, mtime, size) values(?, ?, ?)", tableName))
	if err != nil {
		sow.chStatus.push(RunStatus{err: &err})
	}

	observable := rxgo.FromChannel(sow.typesObjectChannel).BufferWithCount(*sow.app.config.output.Sqlite.commitSize).Map(
		func(_ context.Context, items interface{}) (interface{}, error) {
			tx, err := db.Begin()
			if err != nil {
				sow.chStatus.push(RunStatus{err: &err})
				return nil, err
			}
			my := tx.Stmt(insertStmt)
			for _, item := range items.([]interface{}) {
				obj := item.(types.Object)
				// fmt.Fprintf(os.Stderr, "%T %s %d", *obj.Key, obj.LastModified.String(), obj.Size)
				_, err := my.Exec(*obj.Key, *obj.LastModified, obj.Size)
				if err != nil {
					sow.chStatus.push(RunStatus{err: &err})
				}
			}

			//return sow.deleteObjects(item.([]interface{}))
			err = tx.Commit()
			if err != nil {
				sow.chStatus.push(RunStatus{err: &err})
				return nil, err
			}
			return nil, nil
		},
		rxgo.WithPool(*sow.app.config.output.Sqs.workers),
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
	close(sow.typesObjectChannel)
	sow.waitComplete.Lock()
	sow.waitComplete.Unlock()
}

func makeSqliteOutWriter(app *S3StreamingLister, chStatus MyQueue) OutWriter {
	if *app.config.output.Sqlite.workers < 1 {
		panic("you need at least one worker for sqs")
	}
	// pool := pond.New(*app.config.output.Sqlite.workers, 0, pond.MinWorkers(*app.config.output.Sqlite.workers))
	// chunky, err := makeChunky(&events.S3Event{}, int(*app.config.outputSqs.maxMessageSize))
	// if err != nil {
	// 	chStatus.push(RunStatus{err: &err})
	// }
	sow := SqlLiteOutWriter{
		// chunky:     chunky,
		// chStatus:   chStatus,
		// pool: pool,
		// sqsClients: make(chan *sqs.Client, *app.config.outputSqs.workers),
		chStatus: chStatus,
		app:      app,
		// waitComplete: sync.Mutex{},
	}
	sow.waitComplete.Lock()
	return &sow
}
