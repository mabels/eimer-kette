package main

import (
	"context"
	"log"
	"time"

	"github.com/alitto/pond"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	myq "github.com/mabels/eimer-kette/my-queue"
)

type MyCredentials struct {
	cred aws.Credentials
}

func (my *MyCredentials) Retrieve(ctx context.Context) (aws.Credentials, error) {
	return my.cred, nil
}

type WriteFiles struct {
	clientChan    chan *s3.Client
	doneChan      chan bool
	bucket        Bucket
	workers       int
	lambdaRuntime time.Duration // msec
}

func getClient(wrf *WriteFiles) *s3.Client {
	var client *s3.Client
	select {
	case x := <-wrf.clientChan:
		client = x
	default:
		{
			awsCredProvider := MyCredentials{
				cred: aws.Credentials{
					AccessKeyID:     *wrf.bucket.KeyId,
					SecretAccessKey: *wrf.bucket.AccessKey,
				},
			}
			cfg := aws.Config{
				Credentials: &awsCredProvider,
				Region:      *wrf.bucket.Region,
			}
			client = s3.NewFromConfig(cfg)
		}
	}
	return client
}

func S3putObject(wrf *WriteFiles, item s3.PutObjectInput) {
	client := getClient(wrf)
	_, err := client.PutObject(context.TODO(), &item)
	wrf.clientChan <- client
	if err != nil {
		log.Printf("createS3File: %v:%v", item.Key, err)
	}
	wrf.doneChan <- err == nil
}

func createWorker(wrf *WriteFiles) *myq.MyQueue {
	chi := myq.MakeChannelQueue(wrf.workers)
	pooli := pond.New(wrf.workers, wrf.workers)
	go func() {
		chi.Wait(func(item interface{}) {
			citem := *item.(*s3.PutObjectInput)
			pooli.Submit(func() {
				S3putObject(wrf, citem)
			})
		})
		pooli.Stop()
	}()
	go func() {
		time.Sleep(wrf.lambdaRuntime)
		chi.Stop()
	}()
	return &chi
}
