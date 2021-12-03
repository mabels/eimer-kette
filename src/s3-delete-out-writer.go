package main

import (
	"context"
	"sync/atomic"

	"github.com/alitto/pond"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type S3DeleteOutWriter struct {
	s3Clients chan *s3.Client
	chStatus  Queue
	pool      *pond.WorkerPool
	app       *S3StreamingLister
}

func (sow *S3DeleteOutWriter) setup() OutWriter {
	return sow
}

func (sow *S3DeleteOutWriter) write(items *[]types.Object) {
	sow.pool.Submit(func() {
		var client *s3.Client
		select {
		case x := <-sow.s3Clients:
			client = x
		default:
			atomic.AddInt64(&sow.app.clients.calls.total.newSqs, 1)
			client = s3.NewFromConfig(sow.app.config.output.S3Delete.aws.cfg)
		}
		todelete := s3.DeleteObjectsInput{
			Bucket: sow.app.config.bucket,
		}
		todelete.Delete = &types.Delete{
			Objects: make([]types.ObjectIdentifier, len(*items)),
			Quiet:   true,
		}
		for i, item := range *items {
			todelete.Delete.Objects[i] = types.ObjectIdentifier{
				Key: item.Key,
			}
		}
		// by, _ := json.Marshal(&todelete)
		// oerr := errors.New(fmt.Sprintf("%d:%s", len(*items), string(by)))
		// sow.chStatus.push(RunStatus{err: &oerr})

		_, err := client.DeleteObjects(context.TODO(), &todelete)
		if err != nil {
			sow.chStatus.push(RunStatus{err: &err})
		}
		sow.s3Clients <- client
	})
}

func (sow *S3DeleteOutWriter) done() {
}

func makeS3DeleteOutWriter(app *S3StreamingLister, chStatus Queue) OutWriter {
	if *app.config.output.S3Delete.workers < 1 {
		panic("you need at least one worker for s3 delete")
	}
	pool := pond.New(*app.config.output.S3Delete.workers, *app.config.output.S3Delete.workers)
	sow := S3DeleteOutWriter{
		chStatus:  chStatus,
		pool:      pool,
		app:       app,
		s3Clients: make(chan *s3.Client, *app.config.output.S3Delete.workers),
	}
	return &sow
}
