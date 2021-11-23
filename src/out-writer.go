package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"

	"github.com/alitto/pond"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type Complete struct {
	completed bool
	todo      []types.Object
}

func createSqsMessage(app *S3StreamingLister, todo *[]types.Object) *[]byte {
	records := make([]events.S3EventRecord, len(*todo))
	for i, item := range *todo { //262144 bytes.
		records[i] = events.S3EventRecord{
			S3: events.S3Entity{
				Bucket: events.S3Bucket{
					Name: *app.config.bucket,
				},
				Object: events.S3Object{
					Key: *item.Key,
				},
			},
		}
	}
	event := events.S3Event{Records: records}
	jsonBytes, _ := json.Marshal(event)
	return &jsonBytes
}

func sendSqsMessage(app *S3StreamingLister, jsonBytes *[]byte, chstatus chan RunStatus) {
	jsonStr := string(*jsonBytes)
	_, err := app.output.sqs.SendMessage(context.TODO(), &sqs.SendMessageInput{
		DelaySeconds: *app.config.outputSqs.delay,
		QueueUrl:     app.config.outputSqs.url,
		MessageBody:  &jsonStr,
	})
	if err != nil {
		chstatus <- RunStatus{err: &err}
	}
}

func outWriter(app *S3StreamingLister, tos Complete, chstatus chan RunStatus) {
	chstatus <- RunStatus{outObjects: uint64(len(tos.todo))}
	if *app.config.format == "sqs" {
		// block mode
		jsonBytes := createSqsMessage(app, &tos.todo)

		length := int32(len(*jsonBytes))
		if length > *app.config.outputSqs.maxMessageSize {
			chunkSize := int32(math.Ceil(float64(length / *app.config.outputSqs.maxMessageSize))) + 1
			for chunkSize < int32(len(tos.todo)) {
				newTodo := tos.todo[0:chunkSize:chunkSize]
				jsonBytes = createSqsMessage(app, &newTodo)
				sendSqsMessage(app, jsonBytes, chstatus)
				tos.todo = tos.todo[chunkSize:]
			}
		} else {
			sendSqsMessage(app, jsonBytes, chstatus)
		}
	} else {
		// line mode
		for _, item := range tos.todo {
			if *app.config.format == "mjson" {
				// Add BucketName
				// app.config.bucket
				out, _ := json.Marshal(item)
				fmt.Fprintln(app.output.fileStream, string(out))
			} else if *app.config.format == "awsls" {
				fmt.Fprintf(app.output.fileStream, "%s %10d %s\n",
					item.LastModified.Format("2006-01-02 15:04:05"), item.Size, *item.Key)
			}
		}
	}

	if tos.completed {
		// fmt.Fprintln(os.Stderr, "outWriter-Complete")
		chstatus <- RunStatus{outObjects: 0, completed: true}
	}
}

func outWorker(app *S3StreamingLister, chstatus chan RunStatus) chan Complete {
	cho := make(chan Complete, *app.config.maxKeys)
	poolo := pond.New(*app.config.outWorkers, 0, pond.MinWorkers(*app.config.outWorkers))
	poolo.Submit(func() {
		if *app.config.format == "sqs" {
			app.output.sqs = sqs.New(sqs.Options{
				Region:      *app.config.region,
				Credentials: app.aws.Credentials,
			})
		}
		for items := range cho {
			outWriter(app, items, chstatus)
			if items.completed {
				// fmt.Fprintln(os.Stderr, "Exit-Items", items)
				return
			}
		}
	})
	return cho
}
