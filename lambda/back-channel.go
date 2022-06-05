package lambda

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log"
	"reflect"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/mabels/eimer-kette/models"
	"github.com/mabels/eimer-kette/status"
)

type BackChannel struct {
	Sqs *sqs.Client
	Url string
}

func (bc *BackChannel) SendStatus(st status.RunResult) {
	jsonByte, err := json.Marshal(st)
	if err != nil {
		log.Printf("Marshal error: %v", err)
		return
	}
	bc.SendMessage(&sqs.SendMessageInput{
		// MessageGroupId: aws.String("status.RunStatus"),
		MessageBody: aws.String(string(jsonByte)),
	})
}

func GetGroupId(obj interface{}) *string {
	return aws.String(strings.ReplaceAll(reflect.TypeOf(obj).String(), ".", ""))
}

func (bc *BackChannel) SendMessageJson(obj interface{}) (*sqs.SendMessageOutput, error) {
	jsonObj, err := json.Marshal(obj)
	if err != nil {
		log.Printf("Marshal error: %v", err)
		return nil, err
	}
	jsonObjStr := string(jsonObj)
	return bc.SendMessage(&sqs.SendMessageInput{
		// MessageGroupId: GetGroupId(obj),
		MessageBody: aws.String(jsonObjStr),
	})
}

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
