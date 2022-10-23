package lambda

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"sync"
	"time"

	"github.com/aws/aws-lambda-go/events"
	// c5 "github.com/mabels/c5-envelope/pkg"
	c5 "github.com/mabels/c5-envelope/pkg"
	"github.com/mabels/eimer-kette/models"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/mitchellh/mapstructure"
	// "/frontend"
)

type Frame struct {
	Command string `json:"Command"`
	Payload map[string]interface{}
}

func cleanString(str *string) *string {
	if str == nil {
		return nil
	}
	ret := (*str)[0:1] + "..." + (*str)[len(*str)-1:]
	return &ret
}

// func toJsonString(ptr interface{}) string {
// 	jsonByte, _ := json.Marshal(ptr)
// 	return string(jsonByte)
// }

func cleanAwsCredentials(cmd *map[string]interface{}) *map[string]interface{} {
	for k, v := range *cmd {
		switch k {
		case "AccessKey":
			(*cmd)[k] = cleanString(v.(*string))
		case "KeyId":
			(*cmd)[k] = cleanString(v.(*string))
		case "SessionToken":
			(*cmd)[k] = cleanString(v.(*string))
		default:
			if reflect.TypeOf(v) == reflect.TypeOf(map[string]interface{}{}) {
				cleanAwsCredentials((*cmd)[k].(*map[string]interface{}))
			}
		}
	}
	return cmd
}

func cleanNull(cmd *map[string]interface{}) *map[string]interface{} {
	for k, v := range *cmd {
		if v == nil || (reflect.ValueOf(v).Kind() == reflect.Ptr && reflect.ValueOf(v).IsNil()) {
			delete(*cmd, k)
		} else if reflect.TypeOf(v) == reflect.TypeOf(map[string]interface{}{}) {
			cleanAwsCredentials((*cmd)[k].(*map[string]interface{}))
		}
	}
	return cmd
}

// type StartPayload struct {
// 	// --bucket YOUR_BUCKET_NAME  \
// 	// --outputSqsUrl https://YOUR_QUEUE_URL  \
// 	// --outputSqsMaxMessageSize 20000 \
// 	// --format sqsu
// }

// type StartCmd struct {
// 	Command string `json:"Command"`
// 	Payload StartPayload
// }

// type CreateFiles struct {
// 	Command string `json:"Command"`
// 	Payload int
// }

type HandlerCtx struct {
	SqsClient      *BackChannel
	QueueUrl       string
	S3Clients      *map[string]*s3.Client
	S3ClientsSync  sync.Mutex
	SqsClients     *map[string]BackChannel
	SqsClientsSync sync.Mutex
}

type Result struct {
	Request s3.PutObjectInput
	Result  *s3.PutObjectOutput
	Error   error
}

func (hctx *HandlerCtx) handler(ctx context.Context, sqsEvent events.SQSEvent) error {
	started := time.Now()
	requeue := make([]sqstypes.SendMessageBatchRequestEntry, 0, len(sqsEvent.Records))
	for _, message := range sqsEvent.Records {
		msg := map[string]interface{}{}
		// log.Printf("In: %v", message.Body)
		err := json.Unmarshal([]byte(message.Body), &msg)
		if err != nil {
			return fmt.Errorf("jsonUnmarschal: %v", err)
		}
		_, ok := msg["data"]
		if !ok {
			return fmt.Errorf("no data")
		}
		if reflect.TypeOf(msg["data"]) != reflect.TypeOf(map[string]interface{}{}) {
			return fmt.Errorf("no data data")
		}
		env := c5.EnvelopeT{}
		c5.FromDictEnvelopeT(msg, &env)
		if err != nil {
			return fmt.Errorf("fromDict: %v", err)
		}
		cmdVal := env.Data.Kind
		switch cmdVal {
		case "cmd.CreateFiles":
			cmd := models.CreateFilesPayload{}
			err := mapstructure.Decode(env.Data.Data, &cmd)
			if err != nil {
				return fmt.Errorf("DECODE: %v", err)
			}
			if cmd.ScheduleTime < time.Millisecond*500 {
				cmd.ScheduleTime = 2 * time.Second
			}
			if time.Since(started) > cmd.ScheduleTime {
				id := message.MessageId
				body := message.Body
				requeue = append(requeue, sqstypes.SendMessageBatchRequestEntry{
					Id:          &id,
					MessageBody: &body,
				})
			} else {
				backChannel := hctx.getBackChannel(&cmd.BackChannel)
				// log.Printf("In: %v", cleanCreateFilesPayload(&cmd))
				hctx.createTestHandler(&cmd, started, backChannel)
			}
		case "Lister":
			cmd := models.CmdLister{}
			err := json.Unmarshal([]byte(message.Body), &cmd)
			if err != nil {
				return fmt.Errorf("JsonCreateTestCmd: %v", err)
			}
			if cmd.Payload.ScheduleTime < time.Millisecond*500 {
				cmd.Payload.ScheduleTime = 2 * time.Second
			}

		default:
			log.Printf("unknown command: %s", cmdVal)
		}
	}
	if len(requeue) > 0 {
		log.Printf("Requeued: %v", len(requeue))
		_, err := hctx.SqsClient.SendMessageBatch(&sqs.SendMessageBatchInput{
			QueueUrl: &hctx.QueueUrl,
			Entries:  requeue,
		})
		if err != nil {
			log.Printf("SQS-SendMessage: %v:%v", hctx.QueueUrl, err)
		}
	}
	return nil
}

func HandlerWithContext(handler *HandlerCtx) func(ctx context.Context, sqsEvent events.SQSEvent) error {
	return func(ctx context.Context, sqsEvent events.SQSEvent) error {
		return handler.handler(ctx, sqsEvent)
	}
}

// func main() {
// 	cfg, err := config.LoadDefaultConfig(context.TODO())
// 	if err != nil {
// 		log.Fatalf("configuration error; %v", err)
// 	}
// 	handler := HandlerCtx{
// 		queueUrl:      os.Getenv("AWS_SQS_QUEUE"),
// 		sqsClient:     sqs.NewFromConfig(cfg),
// 		s3ClientsSync: sync.Mutex{},
// 	}
// 	lambda.Start(handlerWithContext(&handler))
// }
