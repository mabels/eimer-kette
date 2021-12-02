package main

import (
	"database/sql"
	"fmt"
	"os"
	"strings"

	"github.com/alitto/pond"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	_ "github.com/mattn/go-sqlite3"
)

type SqlLiteOutWriter struct {
	pool       *pond.WorkerPool
	app        *S3StreamingLister
	db         *sql.DB
	insertStmt *sql.Stmt
}

func (sow *SqlLiteOutWriter) setup() OutWriter {
	if *sow.app.config.outputSqlite.cleanDb {
		os.Remove(*sow.app.config.outputSqlite.filename)
	}
	var err error
	sow.db, err = sql.Open("sqlite3", *sow.app.config.outputSqlite.filename)
	if err != nil {
		panic(err)
	}
	var tableName string
	if *sow.app.config.outputSqlite.sqlTable != "" {
		tableName = *sow.app.config.outputSqlite.sqlTable
	} else {
		tableName = strings.ReplaceAll(*sow.app.config.bucket, ".", "_")
	}
	// defer sow.db.Close()
	sqlStmt := fmt.Sprintf(`
		create table '%s' (
			key text,
			mtime date,
			size integer);
	`, tableName)
	_, err = sow.db.Exec(sqlStmt)
	if err != nil {
		panic(fmt.Sprintf("%q: %s\n", err, sqlStmt))
		// return sow
	}
	sow.insertStmt, err = sow.db.Prepare(
		fmt.Sprintf("insert into %s(key, mtime, size) values(?, ?, ?)", tableName))
	if err != nil {
		panic(err)
		// return sow
	}
	return sow
}

func (sow *SqlLiteOutWriter) write(items *[]types.Object) {
	sow.pool.Submit(func() {
		for _, obj := range *items {
			// fmt.Printf("%s %s %v %d\n", *sow.app.config.bucket, *obj.Key, *obj.LastModified, obj.Size)
			_, err := sow.insertStmt.Exec(*obj.Key, *obj.LastModified, obj.Size)
			if err != nil {
				panic(fmt.Sprintf("insert:%V", err))
			}
		}
	})
	// sow.chunky.append(items)
}

func (sow *SqlLiteOutWriter) done() {
}

func makeSqliteOutWriter(app *S3StreamingLister) OutWriter {
	if *app.config.outputSqlite.workers < 1 {
		panic("you need at least one worker for sqs")
	}
	pool := pond.New(*app.config.outputSqlite.workers, 0, pond.MinWorkers(*app.config.outputSqlite.workers))
	// chunky, err := makeChunky(&events.S3Event{}, int(*app.config.outputSqs.maxMessageSize))
	// if err != nil {
	// 	chStatus.push(RunStatus{err: &err})
	// }
	sow := SqlLiteOutWriter{
		// chunky:     chunky,
		// chStatus:   chStatus,
		pool: pool,
		// sqsClients: make(chan *sqs.Client, *app.config.outputSqs.workers),
		app: app,
	}
	return &sow
}
