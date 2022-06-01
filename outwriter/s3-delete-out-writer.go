package outwriter

import (
	"context"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/reactivex/rxgo/v2"

	"github.com/mabels/eimer-kette/config"
	myq "github.com/mabels/eimer-kette/my-queue"
	"github.com/mabels/eimer-kette/status"
)

type S3DeleteOutWriter struct {
	s3Clients          chan *s3.Client
	chStatus           myq.MyQueue
	app                *config.S3StreamingLister
	typesObjectChannel chan rxgo.Item
	waitComplete       sync.Mutex
	// observable         rxgo.Observable
}

func (sow *S3DeleteOutWriter) deleteObjects(items []interface{}) (*s3.DeleteObjectsOutput, error) {
	var client *s3.Client
	select {
	case x := <-sow.s3Clients:
		client = x
	default:
		sow.app.Clients.Calls.Total.Inc("NewS3")
		client = s3.NewFromConfig(sow.app.Config.Output.S3Delete.Aws.Cfg)
	}
	todelete := s3.DeleteObjectsInput{
		Bucket: sow.app.Config.Bucket,
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
	sow.app.Clients.Calls.Concurrent.Inc("S3Deletes")
	started := time.Now()
	out, err := client.DeleteObjects(context.TODO(), &todelete)
	sow.s3Clients <- client
	sow.app.Clients.Calls.Total.Duration("S3Deletes", started)
	sow.app.Clients.Calls.Concurrent.Dec("S3Deletes")
	if err != nil {
		sow.chStatus.Push(status.RunStatus{Err: &err})
		return nil, nil // might be an problem
	}
	return out, err
}

func (sow *S3DeleteOutWriter) setup() OutWriter {
	sow.typesObjectChannel = make(chan rxgo.Item, *sow.app.Config.Output.S3Delete.ChunkSize**sow.app.Config.Output.S3Delete.Workers)
	observable := rxgo.FromChannel(sow.typesObjectChannel).BufferWithCount(*sow.app.Config.Output.S3Delete.ChunkSize).Map(
		func(_ context.Context, item interface{}) (interface{}, error) {
			return sow.deleteObjects(item.([]interface{}))
		},
		rxgo.WithPool(*sow.app.Config.Output.S3Delete.Workers),
	)
	go func() {
		for range observable.Observe() {
		}
		sow.waitComplete.Unlock()
	}()
	return sow
}

func (sow *S3DeleteOutWriter) write(items *[]types.Object) {
	for _, item := range *items {
		sow.typesObjectChannel <- rxgo.Item{V: item}
	}
}

func (sow *S3DeleteOutWriter) done() {
	// order is important here
	close(sow.typesObjectChannel)
	sow.waitComplete.Lock()
}

func makeS3DeleteOutWriter(app *config.S3StreamingLister, chStatus myq.MyQueue) OutWriter {
	if *app.Config.Output.S3Delete.Workers < 1 {
		panic("you need at least one worker for s3 delete")
	}
	sow := S3DeleteOutWriter{
		chStatus:  chStatus,
		app:       app,
		s3Clients: make(chan *s3.Client, *app.Config.Output.S3Delete.Workers),
	}
	sow.waitComplete.Lock()
	return &sow
}
