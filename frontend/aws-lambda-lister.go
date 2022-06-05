package frontend

import (
	"context"
	"encoding/json"

	"github.com/aws/aws-sdk-go-v2/service/sqs"

	config "github.com/mabels/eimer-kette/config"
	myq "github.com/mabels/eimer-kette/my-queue"
	"github.com/mabels/eimer-kette/status"
)

type ListerCommand struct {
	Command string
	Payload string
}

func AwsLambdaLister(app *config.EimerKette, cho myq.MyQueue, chstatus myq.MyQueue) {
	// Setup BackChannel
	startBackChannel(app, &app.Config.Frontend.AwsLambdaLister.BackChannelQ, cho, chstatus)
	// Issue Command
	// app.Config.Frontend.AwsLambdaLister.CommandQ =
	sqsClient := sqs.NewFromConfig(app.Config.Frontend.AwsLambdaLister.CommandQ.Aws.Cfg)
	msg := ListerCommand{
		Command: "DockerfileLister",
		Payload: "unknown",
	}
	jsonMsg, err := json.Marshal(msg)
	if err != nil {
		chstatus.Push(status.RunStatus{Err: err})
		return
	}
	jsonStr := string(jsonMsg)
	_, err = sqsClient.SendMessage(context.TODO(), &sqs.SendMessageInput{
		MessageBody: &jsonStr,
		QueueUrl:    app.Config.Frontend.AwsLambdaLister.CommandQ.Url,
	})
	if err != nil {
		chstatus.Push(status.RunStatus{Err: err})
		return
	}
}
