package main

import (
	"context"
	"encoding/json"
	"sync/atomic"

	"github.com/alitto/pond"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

type SqsOutWriter struct {
	sqsClients chan *sqs.Client
	chStatus   Queue
	chunky     Chunky
	pool       *pond.WorkerPool
	app        *S3StreamingLister
}

func (sow *SqsOutWriter) setup() OutWriter {
	return sow
}

func (sow *SqsOutWriter) write(items *[]types.Object) {
	// FIXME: use sow.chunky.append(item)
	for _, item := range *items {
		sow.chunky.append(item)
	}
}

func (sow *SqsOutWriter) done() {
	sow.chunky.done(-1)
}

func makeSqsOutWriter(app *S3StreamingLister, chStatus Queue) OutWriter {
	if *app.config.outputSqs.workers < 1 {
		panic("you need at least one worker for sqs")
	}
	pool := pond.New(*app.config.outputSqs.workers, *app.config.outputSqs.workers, pond.MinWorkers(*app.config.outputSqs.workers))
	chunky, err := makeChunky(&events.S3Event{}, int(*app.config.outputSqs.maxMessageSize))
	if err != nil {
		chStatus.push(RunStatus{err: &err})
	}
	sow := SqsOutWriter{
		chunky:     chunky,
		chStatus:   chStatus,
		pool:       pool,
		sqsClients: make(chan *sqs.Client, *app.config.outputSqs.workers),
		app:        app,
	}
	sow.chunky.chunkedFn = func(c *Chunky, collect int) {
		pool.Submit(func() {
			cframe := events.S3Event{} // c.frame.(*events.S3Event)
			cframe.Records = make([]events.S3EventRecord, collect)
			for i := 0; i < collect; i++ {
				item := <-c.records
				cframe.Records[i] = events.S3EventRecord{
					S3: events.S3Entity{
						Bucket: events.S3Bucket{
							Name: *app.config.bucket,
						},
						Object: events.S3Object{
							Key:  *item.(types.Object).Key,
							Size: item.(types.Object).Size,
						},
					},
				}
			}
			jsonBytes, _ := json.Marshal(cframe)
			var client *sqs.Client
			select {
			case x := <-sow.sqsClients:
				client = x
			default:
				atomic.AddInt64(&sow.app.clients.calls.total.newSqs, 1)
				client = sqs.NewFromConfig(sow.app.config.outputSqs.aws.cfg)
			}
			atomic.AddInt64(&sow.app.clients.calls.concurrent.sqsSendMessage, 1)
			sow.sendSqsMessage(app, client, &jsonBytes, chStatus)
			atomic.AddInt64(&sow.app.clients.calls.total.sqsSendMessage, 1)
			atomic.AddInt64(&sow.app.clients.calls.concurrent.sqsSendMessage, -1)
			sow.sqsClients <- client
		})
	}
	return &sow
}

func (sow *SqsOutWriter) sendSqsMessage(app *S3StreamingLister, sqsc *sqs.Client, jsonBytes *[]byte, chstatus Queue) {
	jsonStr := string(*jsonBytes)
	atomic.AddInt64(&sow.app.clients.calls.total.sqsSendMessage, 1)
	atomic.AddInt64(&sow.app.clients.calls.concurrent.sqsSendMessage, 1)
	_, err := sqsc.SendMessage(context.TODO(), &sqs.SendMessageInput{
		DelaySeconds: *app.config.outputSqs.delay,
		QueueUrl:     app.config.outputSqs.url,
		MessageBody:  &jsonStr,
	})
	atomic.AddInt64(&sow.app.clients.calls.concurrent.sqsSendMessage, -1)
	if err != nil {
		chstatus.push(RunStatus{err: &err})
	}
}
