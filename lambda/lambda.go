package lambda

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-lambda-go/events"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"github.com/reactivex/rxgo/v2"
	// "/frontend"
)

type Frame struct {
	Command string `json:"Command"`
	Payload map[string]interface{}
}

type Bucket struct {
	Name      string  `json:"Name"`
	KeyId     *string `json:"KeyId"`
	AccessKey *string `json:"AccessKey"`
	Region    *string `json:"Region"`
}
type CreateTestPayload struct {
	Bucket        Bucket        `json:"Bucket"`
	NumberOfFiles int64         `json:"NumberOfFiles"`
	JobSize       int           `json:"JobSize"`
	JobConcurrent int           `json:"JobConcurrent"`
	SkipCreate    bool          `json:"SkipCreate"`
	ScheduleTime  time.Duration `json:"ScheduleTime"` // default 2000msec
}

type CreateTestCmd struct {
	Command string `json:"Command"`
	Payload CreateTestPayload
}

func cleanString(str *string) *string {
	ret := (*str)[0:1] + "..." + (*str)[len(*str)-1:]
	return &ret
}

func cleanCreateTestPayload(cmd *CreateTestCmd) string {
	ret := *cmd
	ret.Payload.Bucket.KeyId = cleanString(cmd.Payload.Bucket.KeyId)
	ret.Payload.Bucket.AccessKey = cleanString(cmd.Payload.Bucket.AccessKey)
	str, _ := json.Marshal(ret)
	return string(str)
}

type StartPayload struct {
	// --bucket YOUR_BUCKET_NAME  \
	// --outputSqsUrl https://YOUR_QUEUE_URL  \
	// --outputSqsMaxMessageSize 20000 \
	// --format sqsu
}

type StartCmd struct {
	Command string `json:"Command"`
	Payload StartPayload
}

type CreateFiles struct {
	Command string `json:"Command"`
	Payload int
}

type HandlerCtx struct {
	SqsClient     *sqs.Client
	QueueUrl      string
	S3Clients     *map[string]*s3.Client
	S3ClientsSync sync.Mutex
}

type Result struct {
	Request s3.PutObjectInput
	Result  *s3.PutObjectOutput
	Error   error
}

func (hctx *HandlerCtx) writeSingleFiles(cmd *CreateTestCmd, started time.Time) {
	// log.Printf("-1-writeSingleFiles:%v", cleanCreateTestPayload(cmd))
	if cmd.Payload.SkipCreate {
		return
	}
	// log.Printf("-2-writeSingleFiles:%v", cleanCreateTestPayload(cmd))

	// todos := map[string]s3.PutObjectInput{}
	todos := make([]s3.PutObjectInput, cmd.Payload.NumberOfFiles)
	// idProvider := uuid.New()
	// log.Printf("2-Single: %v", cleanCreateTestPayload(cmd))
	// log.Printf("-3-writeSingleFiles:%v", cleanCreateTestPayload(cmd))
	for i := 0; i < int(cmd.Payload.NumberOfFiles); i++ {
		id := strings.ReplaceAll(uuid.New().String(), "-", "/")
		todos[i] = s3.PutObjectInput{
			Key:    &id,
			Bucket: &cmd.Payload.Bucket.Name,
			Body:   strings.NewReader(id),
		}
	}
	// log.Printf("-4-writeSingleFiles:%v", cleanCreateTestPayload(cmd))
	// log.Printf("2-Single: %v", cleanCreateTestPayload(cmd))
	//until := started.Add(cmd.Payload.ScheduleTime)
	observable := rxgo.Just(todos)().Map(
		func(_ context.Context, item interface{}) (interface{}, error) {
			// log.Printf("-5-writeSingleFiles:%v", cleanCreateTestPayload(cmd))
			// log.Printf("3-Single: %v:%v", cleanCreateTestPayload(cmd), items)
			// result := make([]Result, 0, len(items.([]interface{})))
			// log.Printf("4-Single: %v:%d", cleanCreateTestPayload(cmd), len(items.([]interface{})))
			// log.Printf("3-Single: %v", item)
			// for _, item := range items.([]interface{}) {
			if time.Since(started) >= cmd.Payload.ScheduleTime {
				// log.Printf("4-Single: %v", item)
				// log.Printf("-6-writeSingleFiles:%v", cleanCreateTestPayload(cmd))
				return Result{
					Request: item.(s3.PutObjectInput),
				}, nil
			}
			now := time.Now()
			// log.Printf("Write: %v", item.(s3.PutObjectInput).Key)
			out, err := hctx.S3putObject(cmd, item.(s3.PutObjectInput))
			if time.Since(now).Milliseconds() > 100 {
				log.Printf("S3putObject: %f:%s", time.Since(started).Seconds(), *item.(s3.PutObjectInput).Key)
			}
			result := Result{
				Request: item.(s3.PutObjectInput),
				Result:  out,
				Error:   err,
			}
			// log.Printf("-7-writeSingleFiles:%v", cleanCreateTestPayload(cmd))
			// log.Printf("5.1-Single: %v:%d", cleanCreateTestPayload(cmd), time.Since(now))
			return result, nil
		},
		rxgo.WithPool(cmd.Payload.JobConcurrent),
	).Reduce(func(_ context.Context, acc interface{}, item interface{}) (interface{}, error) {
		if acc == nil {
			acc = *cmd
		}
		ocmd := acc.(CreateTestCmd)
		result := item.(Result)
		if result.Error != nil || result.Result != nil {
			ocmd.Payload.NumberOfFiles--
			// log.Printf("6.1-Single: %v:%v", ocmd, item)
			// delete(todos, *result.Request.Key)
		}
		// log.Printf("-8-writeSingleFiles:%v", cleanCreateTestPayload(cmd))
		return ocmd, nil
	})

	// log.Printf("-9-writeSingleFiles:%v", cleanCreateTestPayload(cmd))
	// log.Printf("7-Single:")
	ritem, err := observable.Get()
	// log.Printf("8-Single: %v:%v", ritem, err)
	if err != nil {
		log.Printf("PutObjects:Error %v", err)
		return
	}
	// log.Printf("-10-writeSingleFiles:%v", cleanCreateTestPayload(cmd))
	rcmd := ritem.V.(CreateTestCmd)
	hctx.pushJobSizeCommands(&rcmd, rcmd.Payload.NumberOfFiles, started)
	took := time.Since(started)
	log.Printf("writeSingleFiles:took %f - %d of %d:%v", took.Seconds(), cmd.Payload.NumberOfFiles-rcmd.Payload.NumberOfFiles, cmd.Payload.NumberOfFiles, cleanCreateTestPayload(&rcmd))
}

func (hctx *HandlerCtx) pushJobSizeCommands(cmd *CreateTestCmd, jobSize int64, started time.Time) {
	for done := int64(0); done < cmd.Payload.NumberOfFiles; done += jobSize {
		if time.Since(started) > cmd.Payload.ScheduleTime {
			cmd.Payload.NumberOfFiles = cmd.Payload.NumberOfFiles - done
			log.Printf("pushJobSizeCommands:reschedule: %v", cleanCreateTestPayload(cmd))
			jsonCreateCmd, _ := json.Marshal(*cmd)
			jsonCreateStr := string(jsonCreateCmd)
			_, err := hctx.SqsClient.SendMessage(context.TODO(), &sqs.SendMessageInput{
				QueueUrl:    &hctx.QueueUrl,
				MessageBody: &jsonCreateStr,
			})
			if err != nil {
				log.Printf("SQS-SendMessage: %v:%v", hctx.QueueUrl, err)
			}
			log.Printf("pushJobSizeCommands:took %f", time.Since(started).Seconds())
			return
		}
		my := *cmd
		if done+jobSize > cmd.Payload.NumberOfFiles {
			my.Payload.NumberOfFiles = cmd.Payload.NumberOfFiles - done
		} else {
			my.Payload.NumberOfFiles = jobSize
		}
		// log.Printf("pushSingleCommands:%d of %d:%v", done, cmd.Payload.NumberOfFiles, cleanCreateTestPayload(&my))
		jsonCreateCmd, _ := json.Marshal(my)
		jsonCreateStr := string(jsonCreateCmd)
		_, err := hctx.SqsClient.SendMessage(context.TODO(), &sqs.SendMessageInput{
			QueueUrl:    &hctx.QueueUrl,
			MessageBody: &jsonCreateStr,
		})
		if err != nil {
			log.Printf("SQS-SendMessage: %v:%v", hctx.QueueUrl, err)
		}
	}
}

func (hctx *HandlerCtx) createTestHandler(cmd *CreateTestCmd, started time.Time) {
	parts := cmd.Payload.NumberOfFiles / int64(cmd.Payload.JobSize)
	if parts <= 1 {
		log.Printf("Single: %v", cleanCreateTestPayload(cmd))
		hctx.writeSingleFiles(cmd, started)
	} else if parts <= int64(cmd.Payload.JobSize) {
		log.Printf("Push-1: %v", cleanCreateTestPayload(cmd))
		hctx.pushJobSizeCommands(cmd, int64(cmd.Payload.JobSize), started)
	} else if parts > int64(cmd.Payload.JobSize) {
		log.Printf("Push-2: %v", cleanCreateTestPayload(cmd))
		hctx.pushJobSizeCommands(cmd, int64(parts), started)
	}
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

		val, ok := msg["Command"]
		if ok {
			cmdVal := val.(string)
			switch cmdVal {
			case "Start":
				cmd := StartCmd{}
				err := json.Unmarshal([]byte(message.Body), &cmd)
				if err != nil {
					return fmt.Errorf("JsonStartCmd: %v", err)
				}
			case "CreateTest":
				cmd := CreateTestCmd{}
				err := json.Unmarshal([]byte(message.Body), &cmd)
				if err != nil {
					return fmt.Errorf("JsonCreateTestCmd: %v", err)
				}
				if cmd.Payload.ScheduleTime < time.Millisecond*500 {
					cmd.Payload.ScheduleTime = 2 * time.Second
				}
				if time.Since(started) > cmd.Payload.ScheduleTime {
					id := message.MessageId
					body := message.Body
					requeue = append(requeue, sqstypes.SendMessageBatchRequestEntry{
						Id:          &id,
						MessageBody: &body,
					})
				} else {
					// log.Printf("In: %v", cleanCreateTestPayload(&cmd))
					hctx.createTestHandler(&cmd, started)
				}
			case "lister":
				cmd := CreateTestCmd{}
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
		} else {
			log.Printf("no command")
		}
	}
	if len(requeue) > 0 {
		log.Printf("Requeued: %v", len(requeue))
		_, err := hctx.SqsClient.SendMessageBatch(context.TODO(), &sqs.SendMessageBatchInput{
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
