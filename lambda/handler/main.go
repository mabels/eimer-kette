package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/google/uuid"
	myqueue "github.com/mabels/eimer-kette/my-queue"
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
	Bucket        Bucket `json:"Bucket"`
	NumberOfFiles int64  `json:"NumberOfFiles"`
	JobSize       int    `json:"JobSize"`
	JobConcurrent int    `json:"JobConcurrent"`
	SkipCreate    bool   `json:"SkipCreate"`
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
	sqsClient   *sqs.Client
	queueUrl    string
	writeWorker *myqueue.MyQueue
}

func (self *HandlerCtx) writeSingleFiles(cmd *CreateTestCmd) {
	if cmd.Payload.SkipCreate {
		return
	}
	wrf := &WriteFiles{
		clientChan: make(chan *s3.Client, cmd.Payload.JobConcurrent),
		doneChan:   make(chan bool, cmd.Payload.NumberOfFiles),
		bucket:     cmd.Payload.Bucket,
		workers:    cmd.Payload.JobConcurrent,
	}
	if self.writeWorker == nil {
		worker := createWorker(wrf)
		self.writeWorker = worker
	}
	for i := 0; i < int(cmd.Payload.NumberOfFiles); i++ {
		id := strings.ReplaceAll(uuid.New().String(), "-", "/")
		(*self.writeWorker).Push(&s3.PutObjectInput{
			Key:    &id,
			Bucket: &cmd.Payload.Bucket.Name,
			Body:   strings.NewReader(id),
		})
	}
	for i := 0; i < int(cmd.Payload.NumberOfFiles); i++ {
		<-wrf.doneChan
	}
	//q.Stop()
	out, _ := json.Marshal(&CreateFiles{
		Command: "CreateFiles",
		Payload: int(cmd.Payload.NumberOfFiles),
	})
	log.Println(string(out))
}

func (self *HandlerCtx) pushJobSizeCommands(cmd *CreateTestCmd, jobSize int64) {
	for done := int64(0); done < cmd.Payload.NumberOfFiles; done += jobSize {
		my := *cmd
		if done+jobSize > cmd.Payload.NumberOfFiles {
			my.Payload.NumberOfFiles = cmd.Payload.NumberOfFiles - done
		} else {
			my.Payload.NumberOfFiles = jobSize
		}
		log.Printf("pushSingleCommands:%d of %d:%v", done, cmd.Payload.NumberOfFiles, cleanCreateTestPayload(&my))
		jsonCreateCmd, _ := json.Marshal(my)
		jsonCreateStr := string(jsonCreateCmd)
		_, err := self.sqsClient.SendMessage(context.TODO(), &sqs.SendMessageInput{
			QueueUrl:    &self.queueUrl,
			MessageBody: &jsonCreateStr,
		})
		if err != nil {
			log.Fatalf("SQS-SendMessage: %v:%v", self.queueUrl, err)
		}
	}
}

func (self *HandlerCtx) createTestHandler(cmd *CreateTestCmd) {
	parts := cmd.Payload.NumberOfFiles / int64(cmd.Payload.JobSize)
	if parts <= 1 {
		self.writeSingleFiles(cmd)
	} else if parts <= int64(cmd.Payload.JobSize) {
		self.pushJobSizeCommands(cmd, int64(cmd.Payload.JobSize))
	} else if parts > int64(cmd.Payload.JobSize) {
		self.pushJobSizeCommands(cmd, int64(parts))
	}
}

func (self *HandlerCtx) handler(ctx context.Context, sqsEvent events.SQSEvent) error {
	for _, message := range sqsEvent.Records {
		msg := map[string]interface{}{}
		// log.Printf("In: %v", message.Body)
		err := json.Unmarshal([]byte(message.Body), &msg)
		if err != nil {
			return fmt.Errorf("jsonUnmarschal: %v", err)
		}

		val, ok := msg["Command"]
		if ok {
			switch val.(string) {
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
				self.createTestHandler(&cmd)
			}
		} else {
			return fmt.Errorf("no command")
		}
	}

	return nil
}

func handlerWithContext() func(ctx context.Context, sqsEvent events.SQSEvent) error {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatalf("configuration error; %v", err)
	}
	handler := HandlerCtx{
		queueUrl:  os.Getenv("AWS_SQS_QUEUE"),
		sqsClient: sqs.NewFromConfig(cfg),
	}
	return func(ctx context.Context, sqsEvent events.SQSEvent) error {
		return handler.handler(ctx, sqsEvent)
	}
}

func main() {
	lambda.Start(handlerWithContext())
}
