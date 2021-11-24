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

func (sow *SqsOutWriter) write(tos types.Object) {
	sow.chunky.append(tos)
}

func (sow *SqsOutWriter) done() {
	sow.chunky.done()
}

func makeSqsOutWriter(app *S3StreamingLister, pool *pond.WorkerPool, chStatus Queue) OutWriter {
	chunky, err := makeChunky(&events.S3Event{}, int(*app.config.outputSqs.maxMessageSize))
	if err != nil {
		chStatus.push(RunStatus{err: &err})
	}
	sow := SqsOutWriter{
		chunky:     chunky,
		chStatus:   chStatus,
		pool:       pool,
		sqsClients: make(chan *sqs.Client, *app.config.outWorkers),
		app:        app,
	}
	sow.chunky.chunkedFn = func(c Chunky) {
		pool.Submit(func() {

			cframe := c.frame.(*events.S3Event)
			cframe.Records = make([]events.S3EventRecord, len(c.records))
			for i, item := range c.records {
				cframe.Records[i] = events.S3EventRecord{
					S3: events.S3Entity{
						Bucket: events.S3Bucket{
							Name: *app.config.bucket,
						},
						Object: events.S3Object{
							Key: *item.(types.Object).Key,
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
				client = sqs.New(sqs.Options{
					Region:      *sow.app.config.region,
					Credentials: sow.app.aws.Credentials,
				})
			}
			sow.sendSqsMessage(app, client, &jsonBytes, chStatus)
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
