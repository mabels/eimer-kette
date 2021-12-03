package main

import (
	"context"
	"log"

	"github.com/alitto/pond"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	// "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	// "github.com/aws/aws-sdk-go-v2/service/dynamodb/dynamodbattribute"

	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go/aws"
)

type DynamoOutWriter struct {
	pool *pond.WorkerPool
	app  *S3StreamingLister
	dbs  chan *dynamodb.Client
	// insertStmt *sql.Stmt
}

func (sow *DynamoOutWriter) setup() OutWriter {

	// mySession := session.Must(session.NewSession(&sow.app.config.outputSqs.aws.cfg))
	// sow.db = dynamodb.NewFromConfig(sow.app.config.outputSqs.aws.cfg)
	//.NewConfig(sow.app.config.outputSqs.aws.cfg)

	// if *sow.app.config.outputSqlite.cleanDb {
	// 	os.Remove(*sow.app.config.outputSqlite.filename)
	// }
	// var err error
	// sow.db, err = sql.Open("sqlite3", *sow.app.config.outputSqlite.filename)
	// if err != nil {
	// 	panic(err)
	// }
	// var tableName string
	// if *sow.app.config.outputSqlite.sqlTable != "" {
	// 	tableName = *sow.app.config.outputSqlite.sqlTable
	// } else {
	// 	tableName = strings.ReplaceAll(*sow.app.config.bucket, ".", "_")
	// }
	// // defer sow.db.Close()
	// sqlStmt := fmt.Sprintf(`
	// 	create table '%s' (
	// 		key text,
	// 		mtime date,
	// 		size integer);
	// `, tableName)
	// _, err = sow.db.Exec(sqlStmt)
	// if err != nil {
	// 	panic(fmt.Sprintf("%q: %s\n", err, sqlStmt))
	// 	// return sow
	// }
	// sow.insertStmt, err = sow.db.Prepare(
	// 	fmt.Sprintf("insert into %s(key, mtime, size) values(?, ?, ?)", tableName))
	// if err != nil {
	// 	panic(err)
	// 	// return sow
	// }
	return sow
}

func (sow *DynamoOutWriter) write(items *[]s3types.Object) {
	var client *dynamodb.Client
	// atomic.AddInt64(&app.clients.calls.concurrent.newFromConfig, 1)
	select {
	case x := <-sow.dbs:
		client = x
	default:
		// atomic.AddInt64(&app.clients.calls.total.newFromConfig, 1)
		//fmt.Fprintln(os.Stderr, app.config.listObject.aws.cfg)
		client = dynamodb.NewFromConfig(sow.app.config.outputSqs.aws.cfg)
	}
	sow.pool.Submit(func() {
		for _, item := range *items {
			// av, err := dynamodb.
			// if err != nil {
			// 	log.Fatalf("Got error marshalling new movie item: %s", err)
			// }
			// snippet-end:[dynamodb.go.create_item.assign_struct]

			// snippet-start:[dynamodb.go.create_item.call]
			// Create item in table Movies
			// tableName := "Movies"
			// av := make(map[string]dyntypes.AttributeValue)
			// js, _ := json.Marshal(item)

			av, err := attributevalue.MarshalMap(item)
			if err != nil {
				log.Fatalf("Got error calling MarschalMap: %s", err)
			}

			// fmt.Fprintf(os.Stderr, "XXX=%V:%V:%V:\n%s\n", err, av, item.Key, string(js))

			input := &dynamodb.PutItemInput{
				Item:      av,
				TableName: aws.String("Streamlister"),
			}
			_, err = client.PutItem(context.TODO(), input)
			if err != nil {
				log.Fatalf("Got error calling PutItem: %s", err)
			}

		}
	})
	// sow.chunky.append(items)
}

func (sow *DynamoOutWriter) done() {
}

func makeDynamoOutWriter(app *S3StreamingLister) OutWriter {
	if *app.config.outputSqlite.workers < 1 {
		panic("you need at least one worker for sqs")
	}
	pool := pond.New(*app.config.outputSqlite.workers, 0, pond.MinWorkers(*app.config.outputSqlite.workers))
	// chunky, err := makeChunky(&events.S3Event{}, int(*app.config.outputSqs.maxMessageSize))
	// if err != nil {
	// 	chStatus.push(RunStatus{err: &err})
	// }
	sow := DynamoOutWriter{
		// chunky:     chunky,
		// chStatus:   chStatus,
		pool: pool,
		// sqsClients: make(chan *sqs.Client, *app.config.outputSqs.workers),
		app: app,
		dbs: make(chan *dynamodb.Client, *app.config.outputSqlite.workers),
	}
	return &sow
}
