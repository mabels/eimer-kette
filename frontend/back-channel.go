package frontend

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	config "github.com/mabels/eimer-kette/config"
	myq "github.com/mabels/eimer-kette/my-queue"
	"github.com/mabels/eimer-kette/status"
)

func startBackChannel(app *config.EimerKette, sqsp *config.SqsParams, cho myq.MyQueue, chstatus myq.MyQueue) {
	sqsClient := sqs.NewFromConfig(sqsp.Aws.Cfg)

	go func() {
		for {
			started := time.Now()
			msg, err := sqsClient.ReceiveMessage(context.TODO(), &sqs.ReceiveMessageInput{
				QueueUrl:            sqsp.Url,
				WaitTimeSeconds:     int32(20),
				MaxNumberOfMessages: 10,
			})
			app.Clients.Calls.Total.Duration("SQSReceiveMessgage", started)
			if err != nil {
				fmt.Fprintf(os.Stderr, "SQSReceive>>>%v", err)
				chstatus.Push(status.RunStatus{Err: err})
				time.Sleep(500 * time.Millisecond)
				continue
			}
			for _, msg := range msg.Messages {
				started := time.Now()
				fmt.Fprintf(os.Stderr, "LOOP>>>%s\n", *msg.Body)
				// groupId := msg.Attributes["MessageGroupId"]
				// switch groupId {
				// case "status":
				// 	rstatus := status.RunStatus{}
				// 	err := json.Unmarshal([]byte(*msg.Body), &rstatus)
				// 	if err != nil {
				// 		chstatus.Push(status.RunStatus{Err: err})
				// 		continue
				// 	}
				// 	chstatus.Push(rstatus)
				// case "output":
				// 	rcomplete := Complete{}
				// 	err := json.Unmarshal([]byte(*msg.Body), &rcomplete)
				// 	if err != nil {
				// 		chstatus.Push(status.RunStatus{Err: err})
				// 		continue
				// 	}
				// 	cho.Push(rcomplete)
				// default:
				// 	err := fmt.Errorf("UNKNOWN MessageGroupId:%s", groupId)
				// 	chstatus.Push(status.RunStatus{Err: err})
				// }
				app.Clients.Calls.Total.Duration("SQSProcessMessage", started)
			}
			dbs := make([]types.DeleteMessageBatchRequestEntry, len(msg.Messages))
			for i, m := range msg.Messages {
				dbs[i] = types.DeleteMessageBatchRequestEntry{
					Id:            m.MessageId,
					ReceiptHandle: m.ReceiptHandle,
				}
			}
			started = time.Now()
			sqsClient.DeleteMessageBatch(context.TODO(), &sqs.DeleteMessageBatchInput{
				QueueUrl: sqsp.Url,
				Entries:  dbs,
			})
			app.Clients.Calls.Total.Duration("SQSDeleteMessageBatch", started)

		}
	}()
}
