package lambda

import (
	"context"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/mabels/eimer-kette/models"
)

type MyCredentials struct {
	cred aws.Credentials
}

func (my *MyCredentials) Retrieve(ctx context.Context) (aws.Credentials, error) {
	return my.cred, nil
}

// type WriteFiles struct {
// 	// doneChan      chan bool
// 	bucket Bucket
// 	// workers       int
// 	// lambdaRuntime time.Duration // msec
// }

func (hctx *HandlerCtx) getClient(cmd *models.CmdCreateFiles) (*s3.Client, chan *s3.Client) {
	hctx.S3ClientsSync.Lock()
	defer hctx.S3ClientsSync.Unlock()
	if hctx.S3Clients == nil {
		hctx.S3Clients = &map[string]*s3.Client{}
	}
	client, ok := (*hctx.S3Clients)[cmd.Payload.Bucket.Name]
	if !ok {
		// 	channel = make(chan *s3.Client, cmd.Payload.JobConcurrent)
		// }

		// var client *s3.Client
		// select {
		// case x := <-channel:
		// 	client = x
		// default:
		// 	{
		awsCredProvider := MyCredentials{
			cred: aws.Credentials{
				AccessKeyID:     *cmd.Payload.Bucket.Credentials.KeyId,
				SecretAccessKey: *cmd.Payload.Bucket.Credentials.AccessKey,
			},
		}
		cfg := aws.Config{
			Credentials: &awsCredProvider,
			Region:      *cmd.Payload.Bucket.Credentials.Region,
		}
		client = s3.NewFromConfig(cfg)
		(*hctx.S3Clients)[cmd.Payload.Bucket.Name] = client
	}
	return client, nil
}

func (hctx *HandlerCtx) S3putObject(cmd *models.CmdCreateFiles, item s3.PutObjectInput) (*s3.PutObjectOutput, error) {
	client, _ := hctx.getClient(cmd)
	obs, err := client.PutObject(context.TODO(), &item)
	if err != nil {
		log.Printf("createS3File: %v:%v", item.Key, err)
	}
	return obs, err
}
