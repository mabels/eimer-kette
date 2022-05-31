package main

import (
	"context"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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

func (hctx *HandlerCtx) getClient(cmd *CreateTestCmd) (*s3.Client, chan *s3.Client) {
	hctx.s3ClientsSync.Lock()
	defer hctx.s3ClientsSync.Unlock()
	if hctx.s3Clients == nil {
		hctx.s3Clients = &map[string]*s3.Client{}
	}
	client, ok := (*hctx.s3Clients)[cmd.Payload.Bucket.Name]
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
				AccessKeyID:     *cmd.Payload.Bucket.KeyId,
				SecretAccessKey: *cmd.Payload.Bucket.AccessKey,
			},
		}
		cfg := aws.Config{
			Credentials: &awsCredProvider,
			Region:      *cmd.Payload.Bucket.Region,
		}
		client = s3.NewFromConfig(cfg)
		(*hctx.s3Clients)[cmd.Payload.Bucket.Name] = client
	}
	return client, nil
}

func (hctx *HandlerCtx) S3putObject(cmd *CreateTestCmd, item s3.PutObjectInput) (*s3.PutObjectOutput, error) {
	client, _ := hctx.getClient(cmd)
	obs, err := client.PutObject(context.TODO(), &item)
	if err != nil {
		log.Printf("createS3File: %v:%v", item.Key, err)
	}
	return obs, err
}
