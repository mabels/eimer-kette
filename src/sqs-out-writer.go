package main

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alitto/pond"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/reactivex/rxgo/v2"
)

type SqsOutWriter struct {
	sqsClients         chan *sqs.Client
	chStatus           MyQueue
	app                *S3StreamingLister
	typesObjectChannel chan rxgo.Item
}

func (sow *SqsOutWriter) BufferJsonSize(receive rxgo.Observable, opts ...rxgo.Option) rxgo.Observable {
	// if maxJsonSize <= 0 {
	// 	// return Thrown(IllegalInputError{error: "count must be positive"})
	// }
	ch := make(chan rxgo.Item, *sow.app.config.output.Sqs.workers)

	// return rxgo.Observable(o.parent, o, func() operator {
	// 	return &bufferWithCountOperator{
	// 		count:  count,
	// 		buffer: make([]interface{}, count),
	// 	}
	// }, true, false, opts...)
	frameBytes, err := json.Marshal(events.S3Event{
		Records: []events.S3EventRecord{},
	})
	if err != nil {
		sow.chStatus.push(RunStatus{err: &err})
	}
	records := events.S3Event{
		Records: make([]events.S3EventRecord, 0, *sow.app.config.output.Sqs.chunkSize),
	}
	// recordsIdx := 0
	currentSize := len(frameBytes)
	mutex := sync.Mutex{}

	pool := pond.New(*sow.app.config.output.Sqs.workers, *sow.app.config.output.Sqs.workers)
	receive.DoOnNext(func(object interface{}) {
		pool.Submit(func() {
			event := events.S3EventRecord{
				EventVersion: "V1",                                  // string              `json:"eventVersion"`
				EventSource:  "s3-streaming-lister",                 //      string              `json:"eventSource"`
				AWSRegion:    *sow.app.config.output.Sqs.aws.region, //         string              `json:"awsRegion"`
				EventTime:    time.Now(),                            //       time.Time           `json:"eventTime"`
				EventName:    "ObjectCreated:Put",                   //         string              `json:"eventName"`
				PrincipalID: events.S3UserIdentity{
					PrincipalID: "s3-streaming-lister",
				},
				RequestParameters: events.S3RequestParameters{
					SourceIPAddress: "8.8.8.8",
				}, //`json:"requestParameters"`
				ResponseElements: map[string]string{}, //   `json:"responseElements"`
				S3: events.S3Entity{
					Bucket: events.S3Bucket{
						Name: *sow.app.config.bucket,
					},
					Object: events.S3Object{
						Key:  *object.(types.Object).Key,
						Size: object.(types.Object).Size,
					},
				},
			}
			eventBytes, err := json.Marshal(event)
			if err != nil {
				sow.chStatus.push(RunStatus{err: &err})
			}
			eventSize := len(eventBytes)
			if currentSize != len(frameBytes) {
				eventSize += len(",")
			}
			mutex.Lock()
			if currentSize+eventSize > *sow.app.config.output.Sqs.maxMessageSize {
				out, err := json.Marshal(records)
				if err != nil {
					sow.chStatus.push(RunStatus{err: &err})
				}
				// fmt.Fprintln(os.Stderr, len(out), len(records.Records))
				ch <- rxgo.Item{V: string(out)}
				records = events.S3Event{
					Records: make([]events.S3EventRecord, 0, *sow.app.config.output.Sqs.chunkSize),
				}
				currentSize = len(frameBytes)
				// recordsIdx = 0
			}
			currentSize += eventSize
			records.Records = append(records.Records, event)
			mutex.Unlock()
		})

	})
	receive.DoOnError(func(e error) {
		close(ch)
	})
	receive.DoOnCompleted(func() {
		close(ch)
	})
	return rxgo.FromChannel(ch)
}

// type bufferWithCountOperator struct {
// 	count  int
// 	iCount int
// 	buffer []interface{}
// }

// func (op *bufferWithCountOperator) next(ctx context.Context, item Item, dst chan<- Item, _ operatorOptions) {
// 	op.buffer[op.iCount] = item.V
// 	op.iCount++
// 	if op.iCount == op.count {
// 		Of(op.buffer).SendContext(ctx, dst)
// 		op.iCount = 0
// 		op.buffer = make([]interface{}, op.count)
// 	}
// }

// func (op *bufferWithCountOperator) err(ctx context.Context, item Item, dst chan<- Item, operatorOptions operatorOptions) {
// 	defaultErrorFuncOperator(ctx, item, dst, operatorOptions)
// }

// func (op *bufferWithCountOperator) end(ctx context.Context, dst chan<- Item) {
// 	if op.iCount != 0 {
// 		Of(op.buffer[:op.iCount]).SendContext(ctx, dst)
// 	}
// }

// func (op *bufferWithCountOperator) gatherNext(_ context.Context, _ Item, _ chan<- Item, _ operatorOptions) {
// }

func (sow *SqsOutWriter) sqsSendMessage(body *string) (*sqs.SendMessageOutput, error) {
	var client *sqs.Client
	select {
	case x := <-sow.sqsClients:
		client = x
	default:
		atomic.AddInt64(&sow.app.clients.calls.total.newSqs, 1)
		client = sqs.NewFromConfig(sow.app.config.output.Sqs.aws.cfg)
	}
	atomic.AddInt64(&sow.app.clients.calls.total.sqsSendMessage, 1)
	atomic.AddInt64(&sow.app.clients.calls.concurrent.sqsSendMessage, 1)
	out, err := client.SendMessage(context.TODO(), &sqs.SendMessageInput{
		DelaySeconds: *sow.app.config.output.Sqs.delay,
		QueueUrl:     sow.app.config.output.Sqs.url,
		MessageBody:  body,
	})
	atomic.AddInt64(&sow.app.clients.calls.concurrent.sqsSendMessage, -1)
	if err != nil {
		sow.chStatus.push(RunStatus{err: &err})
	}
	sow.sqsClients <- client
	return out, err
}

func (sow *SqsOutWriter) setup() OutWriter {
	sow.typesObjectChannel = make(chan rxgo.Item, *sow.app.config.output.Sqs.chunkSize**sow.app.config.output.Sqs.workers)
	observable := sow.BufferJsonSize(rxgo.FromChannel(sow.typesObjectChannel)).Map(
		func(_ context.Context, item interface{}) (interface{}, error) {
			// fmt.Fprintln(os.Stderr, item)
			// out := "TODO"
			json := item.(string)
			out, err := sow.sqsSendMessage(&json)
			if err != nil {
				sow.chStatus.push(RunStatus{err: &err})
			}
			return out, err
			// return nil, nil
		},
		rxgo.WithPool(*sow.app.config.output.Sqs.workers),
	)
	go func() {
		for range observable.Observe() {
		}
	}()
	return sow
}

func (sow *SqsOutWriter) write(items *[]types.Object) {
	for _, item := range *items {
		sow.typesObjectChannel <- rxgo.Item{V: item}
	}
}

func (sow *SqsOutWriter) done() {
	close(sow.typesObjectChannel)
}

func makeSqsOutWriter(app *S3StreamingLister, chStatus MyQueue) OutWriter {
	if *app.config.output.Sqs.workers < 1 {
		panic("you need at least one worker for s3 delete")
	}
	sow := SqsOutWriter{
		chStatus:   chStatus,
		app:        app,
		sqsClients: make(chan *sqs.Client, *app.config.output.Sqs.workers),
	}
	return &sow
}
