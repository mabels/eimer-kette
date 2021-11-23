package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/alitto/pond"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

type Complete struct {
	completed bool
	todo      []types.Object
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

func outWriterSqs(app *S3StreamingLister, tos Complete, chstatus chan RunStatus) {
	chunky, _ := makeChunky(&events.S3Event{}, int(*app.config.outputSqs.maxMessageSize), func(c Chunky) {
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
		sendSqsMessage(app, &jsonBytes, chstatus)
	})
	for _, item := range tos.todo {
		chunky.append(item)
	}
	chunky.done()
}

func outWriterMjson(app *S3StreamingLister, tos Complete) {
	for _, item := range tos.todo {
		out, _ := json.Marshal(item)
		fmt.Fprintln(app.output.fileStream, string(out))
	}
}

func outWriterAwsls(app *S3StreamingLister, tos Complete) {
	for _, item := range tos.todo {
		fmt.Fprintf(app.output.fileStream, "%s %10d %s\n",
			item.LastModified.Format("2006-01-02 15:04:05"), item.Size, *item.Key)
	}
}

func outWriter(app *S3StreamingLister, tos Complete, chstatus chan RunStatus) {
	chstatus <- RunStatus{outObjects: uint64(len(tos.todo))}
	if *app.config.format == "sqs" {
		outWriterSqs(app, tos, chstatus)
	} else if *app.config.format == "mjson" {
		outWriterMjson(app, tos)
	} else if *app.config.format == "awsls" {
		outWriterAwsls(app, tos)
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
