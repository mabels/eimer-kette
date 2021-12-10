package outwriter

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/reactivex/rxgo/v2"

	"github.com/mabels/s3-streaming-lister/config"
	myq "github.com/mabels/s3-streaming-lister/my-queue"
	"github.com/mabels/s3-streaming-lister/status"
)

type SqsOutWriter struct {
	sqsClients         chan *sqs.Client
	chStatus           myq.MyQueue
	app                *config.S3StreamingLister
	typesObjectChannel chan rxgo.Item
	waitComplete       sync.Mutex
	writerCnt          int
}

func (sow *SqsOutWriter) BufferJsonSize(receive rxgo.Observable, opts ...rxgo.Option) rxgo.Observable {
	// if maxJsonSize <= 0 {
	// 	// return Thrown(IllegalInputError{error: "count must be positive"})
	// }
	ch := make(chan rxgo.Item, *sow.app.Config.Output.Sqs.Workers)

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
		sow.chStatus.Push(status.RunStatus{Err: &err})
	}
	go func() {
		records := events.S3Event{
			Records: make([]events.S3EventRecord, 0, *sow.app.Config.Output.Sqs.ChunkSize),
		}
		currentSize := len(frameBytes)
		mutex := sync.Mutex{}
		// pool := pond.New(*sow.app.Config.Output.Sqs.Workers, *sow.app.Config.Output.Sqs.Workers)
		fmt.Fprintln(os.Stderr, "sqsobserver:enter")
		for item := range receive.Observe() {
			if item.V != nil {
				object := item.V.(types.Object)
				sow.app.Clients.Calls.Total.Inc("SqsDoNext")
				event := events.S3EventRecord{
					EventVersion: "V1",                                  // string              `json:"eventVersion"`
					EventSource:  "s3-streaming-lister",                 //      string              `json:"eventSource"`
					AWSRegion:    *sow.app.Config.Output.Sqs.Aws.Region, //         string              `json:"awsRegion"`
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
							Name: *sow.app.Config.Bucket,
						},
						Object: events.S3Object{
							Key:  *object.Key,
							Size: object.Size,
						},
					},
				}
				eventBytes, err := json.Marshal(event)
				if err != nil {
					sow.chStatus.Push(status.RunStatus{Err: &err})
				}
				eventSize := len(eventBytes)
				if currentSize != len(frameBytes) {
					eventSize += len(",")
				}
				mutex.Lock()
				if currentSize+eventSize >= *sow.app.Config.Output.Sqs.MaxMessageSize {
					sow.app.Clients.Calls.Concurrent.Add(len(records.Records), "SqsRecords")
					sow.app.Clients.Calls.Total.Add(len(records.Records), "SqsRecords")
					// fmt.Fprintf(os.Stderr, "toSend:%d\n", len(records.Records))
					ch <- rxgo.Item{V: records}
					records = events.S3Event{
						Records: make([]events.S3EventRecord, 0, *sow.app.Config.Output.Sqs.ChunkSize),
					}
					currentSize = len(frameBytes)
					// recordsIdx = 0
				}
				currentSize += eventSize
				records.Records = append(records.Records, event)
				mutex.Unlock()
			}
			if item.E != nil {
				sow.chStatus.Push(status.RunStatus{Err: &item.E})
				close(ch)
			}
		}
		fmt.Fprintln(os.Stderr, "sqsobserver:leave")
		sow.app.Clients.Calls.Concurrent.Add(len(records.Records), "SqsRecords")
		sow.app.Clients.Calls.Total.Add(len(records.Records), "SqsRecords")
		ch <- rxgo.Item{V: records}
		close(ch)
	}()
	return rxgo.FromChannel(ch)
}

func (sow *SqsOutWriter) sqsSendMessage(s3event *events.S3Event) (*sqs.SendMessageOutput, error) {
	jsonBytes, err := json.Marshal(s3event)
	if err != nil {
		sow.chStatus.Push(status.RunStatus{Err: &err})
	}
	jsonStr := string(jsonBytes)

	var client *sqs.Client
	select {
	case x := <-sow.sqsClients:
		client = x
	default:
		sow.app.Clients.Calls.Total.Inc("NewSqs")
		client = sqs.NewFromConfig(sow.app.Config.Output.Sqs.Aws.Cfg)
	}
	sow.app.Clients.Calls.Concurrent.Inc("SqsSendMessage")
	started := time.Now()
	out, err := client.SendMessage(context.TODO(), &sqs.SendMessageInput{
		DelaySeconds: *sow.app.Config.Output.Sqs.Delay,
		QueueUrl:     sow.app.Config.Output.Sqs.Url,
		MessageBody:  &jsonStr,
	})
	sow.app.Clients.Calls.Concurrent.Add(-len(s3event.Records), "SqsRecords")
	sow.app.Clients.Calls.Total.Duration("SqsSendMessage", started)
	sow.app.Clients.Calls.Concurrent.Dec("SqsSendMessage")
	sow.sqsClients <- client
	if err != nil {
		sow.app.Clients.Calls.Error.Inc("SqsSendMessage")
		sow.chStatus.Push(status.RunStatus{Err: &err})
	}
	return out, err
}

func (sow *SqsOutWriter) setup() OutWriter {
	sow.typesObjectChannel = make(chan rxgo.Item, *sow.app.Config.Output.Sqs.ChunkSize**sow.app.Config.Output.Sqs.Workers)
	observable := sow.BufferJsonSize(rxgo.FromChannel(sow.typesObjectChannel)).Map(
		func(_ context.Context, item interface{}) (interface{}, error) {
			// out := "TODO"
			// json, err := json.Marshal(events.S3Event)
			s3Events := item.(events.S3Event)
			// fmt.Fprintln(os.Stderr, "SendSqs:", len(s3Events.Records))
			out, err := sow.sqsSendMessage(&s3Events)
			if err != nil {
				sow.chStatus.Push(status.RunStatus{Err: &err})
			}
			return out, err
			// return nil, nil
		},
		rxgo.WithPool(*sow.app.Config.Output.Sqs.Workers),
	)
	go func() {
		fmt.Fprintln(os.Stderr, "setup-runner-pre")
		for range observable.Observe() {
		}
		fmt.Fprintln(os.Stderr, "setup-runner-post")
		sow.waitComplete.Unlock()
	}()
	return sow
}

// writeCnt := 0
func (sow *SqsOutWriter) write(items *[]types.Object) {
	for _, item := range *items {
		// writeCnt++
		sow.typesObjectChannel <- rxgo.Item{V: item}
	}
	sow.writerCnt += len(*items)
	// fmt.Fprintln(os.Stderr, "SqsWriterCnt:", sow.writerCnt)
}

func (sow *SqsOutWriter) done() {
	close(sow.typesObjectChannel)
	fmt.Fprintln(os.Stderr, "SqsWriterDone:enter")
	sow.waitComplete.Lock()
	// sow.waitComplete.Unlock()
	fmt.Fprintln(os.Stderr, "SqsWriterDone:leave")
}

func makeSqsOutWriter(app *config.S3StreamingLister, chStatus myq.MyQueue) OutWriter {
	if *app.Config.Output.Sqs.Workers < 1 {
		err := fmt.Errorf("you need at least one worker for s3 delete")
		chStatus.Push(status.RunStatus{Err: &err})
	}
	sow := SqsOutWriter{
		chStatus:   chStatus,
		app:        app,
		sqsClients: make(chan *sqs.Client, *app.Config.Output.Sqs.Workers),
	}
	sow.waitComplete.Lock()
	return &sow
}
