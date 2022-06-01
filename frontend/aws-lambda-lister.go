package frontend

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"

	config "github.com/mabels/eimer-kette/config"
	myq "github.com/mabels/eimer-kette/my-queue"
	"github.com/mabels/eimer-kette/status"
)

func startBackChannel(app *config.S3StreamingLister, cho myq.MyQueue, chstatus myq.MyQueue) {
	sqsClient := sqs.NewFromConfig(app.Config.Frontend.AwsLambdaLister.BackChannelQ.Aws.Cfg)

	go func() {
		for {
			msg, err := sqsClient.ReceiveMessage(context.TODO(), &sqs.ReceiveMessageInput{
				QueueUrl:            app.Config.Frontend.AwsLambdaLister.BackChannelQ.Url,
				MaxNumberOfMessages: 10,
			})
			if err != nil {
				chstatus.Push(status.RunStatus{Err: &err})
				time.Sleep(500 * time.Millisecond)
				continue
			}
			for _, msg := range msg.Messages {
				groupId := msg.Attributes["MessageGroupId"]
				switch groupId {
				case "status":
					rstatus := status.RunStatus{}
					err := json.Unmarshal([]byte(*msg.Body), &rstatus)
					if err != nil {
						chstatus.Push(status.RunStatus{Err: &err})
						continue
					}
					chstatus.Push(rstatus)
				case "output":
					rcomplete := Complete{}
					err := json.Unmarshal([]byte(*msg.Body), &rcomplete)
					if err != nil {
						chstatus.Push(status.RunStatus{Err: &err})
						continue
					}
					cho.Push(rcomplete)
				default:
					err := fmt.Errorf("UNKNOWN MessageGroupId:%s", groupId)
					chstatus.Push(status.RunStatus{Err: &err})
				}
			}

		}
	}()
}

type ListerCommand struct {
	Command string
	Payload string
}

func AwsLambdaLister(app *config.S3StreamingLister, cho myq.MyQueue, chstatus myq.MyQueue) {
	// Setup BackChannel
	startBackChannel(app, cho, chstatus)
	// Issue Command
	// app.Config.Frontend.AwsLambdaLister.CommandQ =
	sqsClient := sqs.NewFromConfig(app.Config.Frontend.AwsLambdaLister.CommandQ.Aws.Cfg)
	msg := ListerCommand{
		Command: "lister",
		Payload: "unknown",
	}
	jsonMsg, err := json.Marshal(msg)
	if err != nil {
		chstatus.Push(status.RunStatus{Err: &err})
		return
	}
	jsonStr := string(jsonMsg)
	_, err = sqsClient.SendMessage(context.TODO(), &sqs.SendMessageInput{
		MessageBody: &jsonStr,
		QueueUrl:    app.Config.Frontend.AwsLambdaLister.CommandQ.Url,
	})
	if err != nil {
		chstatus.Push(status.RunStatus{Err: &err})
		return
	}
}
