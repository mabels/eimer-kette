package lambda

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/mabels/eimer-kette/config"
	"github.com/mabels/eimer-kette/models"
	myq "github.com/mabels/eimer-kette/my-queue"
	"github.com/mabels/eimer-kette/status"

	c5 "github.com/mabels/c5-envelope/pkg"

	"github.com/fatih/structs"
)

type BackChannel struct {
	Sqs *sqs.Client
	Url string
}

type strErrorRunResult struct {
	Action  string
	Took    time.Duration
	Related *map[string]interface{}
	Err     *string
	Fatal   *string
}

func (bc *BackChannel) SendResult(st status.RunResult) (*sqs.SendMessageOutput, error) {
	var fatal *string
	if st.Fatal != nil {
		my := st.Fatal.Error()
		fatal = &my
	}
	var errStr *string
	if st.Err != nil {
		my := st.Err.Error()
		errStr = &my
	}
	var related *map[string]interface{}
	if st.Related != nil {
		my := structs.Map(st.Related)
		related = &my
	}
	strResult := structs.Map(strErrorRunResult{
		Action:  st.Action,
		Took:    st.Took,
		Related: related,
		Err:     errStr,
		Fatal:   fatal,
	})
	env := c5.NewSimpleEnvelope(&c5.SimpleEnvelopeProps{
		Data: c5.PayloadT1{
			Data: *cleanNull(cleanAwsCredentials(&strResult)),
			Kind: GetGroupId(st),
		},
	})
	out := env.AsJson()
	o, err := bc.SendMessage(&sqs.SendMessageInput{
		// MessageGroupId: aws.String("status.RunStatus"),
		MessageBody: out,
	})
	return o, err
}

func (bc *BackChannel) SendCmdCreateFiles(cmd *models.CreateFilesPayload) (*sqs.SendMessageOutput, error) {
	env := c5.NewSimpleEnvelope(&c5.SimpleEnvelopeProps{
		Data: c5.PayloadT1{
			Data: structs.Map(cmd),
			Kind: GetGroupId(*cmd),
		},
	})
	str := aws.String(*env.AsJson())
	// fmt.Fprintf(os.Stderr, "SendCmdCreateFiles: %s\n", *str)
	return bc.SendMessage(&sqs.SendMessageInput{
		// MessageGroupId: aws.String("status.RunStatus"),
		MessageBody: str,
	})
}

func GetGroupId(obj interface{}) string {
	return reflect.TypeOf(obj).String()
}

// func (bc *BackChannel) sendMessageJson(obj interface{}) (*sqs.SendMessageOutput, error) {
// 	jsonObj, err := json.Marshal(obj)
// 	if err != nil {
// 		log.Printf("Marshal error: %v", err)
// 		return nil, err
// 	}
// 	jsonObjStr := string(jsonObj)
// 	return bc.SendMessage(&sqs.SendMessageInput{
// 		// MessageGroupId: GetGroupId(obj),
// 		MessageBody: aws.String(jsonObjStr),
// 	})
// }

func (bc *BackChannel) SendMessageBatch(batch *sqs.SendMessageBatchInput) (*sqs.SendMessageBatchOutput, error) {
	clone := *batch
	clone.QueueUrl = &bc.Url
	return bc.Sqs.SendMessageBatch(context.TODO(), &clone)
}

func (bc *BackChannel) SendMessage(msg *sqs.SendMessageInput) (*sqs.SendMessageOutput, error) {
	clone := *msg
	clone.QueueUrl = &bc.Url
	return bc.Sqs.SendMessage(context.TODO(), &clone)
}

func (hctx *HandlerCtx) getBackChannel(sqp *models.SqsParams) *BackChannel {
	o := sha256.New()
	o.Write([]byte(sqp.Url))
	o.Write([]byte(*sqp.Credentials.AccessKey))
	o.Write([]byte(*sqp.Credentials.KeyId))
	o.Write([]byte(*sqp.Credentials.Region))
	key := hex.EncodeToString(o.Sum(nil))
	if hctx.SqsClients == nil {
		hctx.SqsClients = &map[string]BackChannel{}
	}
	hctx.SqsClientsSync.Lock()
	backChannel, ok := (*hctx.SqsClients)[key]
	if !ok {
		awsCredProvider := MyCredentials{
			cred: aws.Credentials{
				AccessKeyID:     *sqp.Credentials.KeyId,
				SecretAccessKey: *sqp.Credentials.AccessKey,
				SessionToken:    *sqp.Credentials.SessionToken,
			},
		}
		// fmt.Fprintln(os.Stderr, "Region=", *awsParams.region)
		cfg := aws.Config{
			Credentials: &awsCredProvider,
			// func (fn CredentialsProviderFunc) Retrieve(ctx context.Context) (Credentials, error) {
			// return fn(ctx)
			// },
			Region: *sqp.Credentials.Region,
		}
		backChannel = BackChannel{
			Sqs: sqs.NewFromConfig(cfg),
			Url: sqp.Url,
		}
		(*hctx.SqsClients)[key] = backChannel
	}
	hctx.SqsClientsSync.Unlock()
	return &backChannel
}

func StartBackChannel(app *config.EimerKette, sqsp *config.SqsParams, cho myq.MyQueue, chstatus myq.MyQueue) {
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
				chstatus.Push(status.RunStatus{Err: err})
				time.Sleep(500 * time.Millisecond)
				continue
			}
			for _, message := range msg.Messages {
				started := time.Now()
				msg := map[string]interface{}{}
				// fmt.Fprintf(os.Stderr, "LOOP>>>%s\n", *msg.Body)
				err := json.Unmarshal([]byte(*message.Body), &msg)
				if err != nil {
					chstatus.Push(status.RunStatus{Err: err})
					return
				}
				_, ok := msg["data"]
				if !ok {
					chstatus.Push(status.RunStatus{Err: fmt.Errorf("no data")})
					return
				}
				if reflect.TypeOf(msg["data"]) != reflect.TypeOf(map[string]interface{}{}) {
					chstatus.Push(status.RunStatus{Err: fmt.Errorf("no data data")})
					return
				}
				out, _ := json.Marshal(msg)
				fmt.Fprintf(os.Stderr, "SQSReceive>>>%s\n%s\n", *message.Body, out)
				env := c5.EnvelopeT{}
				err = c5.FromDictEnvelopeT(msg, &env)
				if err != nil {
					chstatus.Push(status.RunStatus{Err: fmt.Errorf("fromDict: %v", err)})
					return
				}
				cmdVal := env.Data.Kind
				switch cmdVal {
				default:
					chstatus.Push(status.RunStatus{Err: fmt.Errorf("UNHANDLED Type: %s", cmdVal)})
				}
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
			if len(msg.Messages) > 0 {
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

		}
	}()
}
