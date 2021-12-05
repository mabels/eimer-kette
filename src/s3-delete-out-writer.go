package main

import (
	"context"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/reactivex/rxgo/v2"
)

type S3DeleteOutWriter struct {
	s3Clients          chan *s3.Client
	chStatus           MyQueue
	app                *S3StreamingLister
	typesObjectChannel chan rxgo.Item
	// observable         rxgo.Observable
}

func (sow *S3DeleteOutWriter) deleteObjects(items []interface{}) (*s3.DeleteObjectsOutput, error) {
	var client *s3.Client
	select {
	case x := <-sow.s3Clients:
		client = x
	default:
		atomic.AddInt64(&sow.app.clients.calls.total.newS3, 1)
		client = s3.NewFromConfig(sow.app.config.output.S3Delete.aws.cfg)
	}
	todelete := s3.DeleteObjectsInput{
		Bucket: sow.app.config.bucket,
	}
	todelete.Delete = &types.Delete{
		Objects: make([]types.ObjectIdentifier, len(items)),
		Quiet:   true,
	}
	for i, item := range items {
		todelete.Delete.Objects[i] = types.ObjectIdentifier{
			Key: item.(types.Object).Key,
		}
	}
	atomic.AddInt64(&sow.app.clients.calls.total.s3Deletes, 1)
	atomic.AddInt64(&sow.app.clients.calls.concurrent.s3Deletes, 1)
	out, err := client.DeleteObjects(context.TODO(), &todelete)
	atomic.AddInt64(&sow.app.clients.calls.concurrent.s3Deletes, -1)
	if err != nil {
		sow.chStatus.push(RunStatus{err: &err})
	}
	sow.s3Clients <- client
	return out, err
}

func (sow *S3DeleteOutWriter) setup() OutWriter {
	sow.typesObjectChannel = make(chan rxgo.Item, *sow.app.config.output.S3Delete.chunkSize**sow.app.config.output.S3Delete.workers)
	observable := rxgo.FromChannel(sow.typesObjectChannel).BufferWithCount(*sow.app.config.output.S3Delete.chunkSize).Map(
		func(_ context.Context, item interface{}) (interface{}, error) {
			return sow.deleteObjects(item.([]interface{}))
		},
		rxgo.WithPool(*sow.app.config.output.S3Delete.workers),
	)
	go func() {
		for range observable.Observe() {
		}
	}()
	return sow
}

func (sow *S3DeleteOutWriter) write(items *[]types.Object) {
	for _, item := range *items {
		sow.typesObjectChannel <- rxgo.Item{V: item}
	}
}

func (sow *S3DeleteOutWriter) done() {
	close(sow.typesObjectChannel)
}

func makeS3DeleteOutWriter(app *S3StreamingLister, chStatus MyQueue) OutWriter {
	if *app.config.output.S3Delete.workers < 1 {
		panic("you need at least one worker for s3 delete")
	}
	sow := S3DeleteOutWriter{
		chStatus:  chStatus,
		app:       app,
		s3Clients: make(chan *s3.Client, *app.config.output.S3Delete.workers),
	}
	return &sow
}
