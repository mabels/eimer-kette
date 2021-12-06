package frontend

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync/atomic"

	"github.com/alitto/pond"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	config "github.com/mabels/s3-streaming-lister/config"
	myq "github.com/mabels/s3-streaming-lister/my-queue"
	"github.com/mabels/s3-streaming-lister/status"
)

func S3Lister(app *config.S3StreamingLister, input s3.ListObjectsV2Input, chi myq.MyQueue, cho myq.MyQueue, chstatus myq.MyQueue) {
	var client *s3.Client
	atomic.AddInt64(&app.Clients.Calls.Concurrent.NewFromConfig, 1)
	select {
	case x := <-app.Clients.Channels:
		client = x
	default:
		atomic.AddInt64(&app.Clients.Calls.Total.NewFromConfig, 1)
		//fmt.Fprintln(os.Stderr, app.Config.listObject.aws.cfg)
		client = s3.NewFromConfig(app.Config.ListObject.Aws.Cfg)
	}
	// fmt.Fprintln(os.Stderr, "Pre=", *input.Prefix)
	atomic.AddInt64(&app.Clients.Calls.Concurrent.NewFromConfig, -1)
	atomic.AddInt64(&app.Clients.Calls.Total.ListObjectsV2, 1)
	atomic.AddInt64(&app.Clients.Calls.Concurrent.ListObjectsV2, 1)
	resp, err := client.ListObjectsV2(context.TODO(), &input)
	if err != nil {
		chstatus.Push(status.RunStatus{Err: &err})
		// fmt.Fprintf(os.Stderr, "Got error retrieving list of objects:%s", *input.Bucket)
		// fmt.Fprintln(os.Stderr, err)
		return
	}
	app.Clients.Channels <- client
	atomic.AddInt64(&app.Clients.Calls.Concurrent.ListObjectsV2Input, -1)
	atomic.AddInt64(&app.Clients.Calls.Concurrent.ListObjectsV2, -1)

	if resp.NextContinuationToken != nil {
		atomic.AddInt64(&app.Clients.Calls.Total.ListObjectsV2Input, 1)
		if *app.Config.Strategy == "delimiter" {
			DelimiterStrategy(app, input.Prefix, resp.NextContinuationToken, chi)
		} else if *app.Config.Strategy == "letter" {
			atomic.AddInt32(&app.InputConcurrent, -1)
			atomic.AddInt32(&app.InputConcurrent, int32(len(*app.Config.Prefixes)))
			SingleLetterStrategy(app, input.Prefix, chi)
			return
		}
	} else {
		atomic.AddInt32(&app.InputConcurrent, -1)
	}

	atomic.AddInt64(&app.Clients.Calls.Total.ListObjectsV2Input, int64(len(resp.CommonPrefixes)))
	atomic.AddInt64(&app.Clients.Calls.Concurrent.ListObjectsV2Input, int64(len(resp.CommonPrefixes)))
	for _, item := range resp.CommonPrefixes {
		if *app.Config.Strategy == "delimiter" {
			atomic.AddInt32(&app.InputConcurrent, 1)
			DelimiterStrategy(app, item.Prefix, nil, chi)
		} else if *app.Config.Strategy == "letter" {
			out, _ := json.Marshal(resp.CommonPrefixes)
			fmt.Fprintln(os.Stderr, string(out))
			panic("letter should not go to this")
		}
	}
	// fmt.Fprintln(os.Stderr, "Post=", *input.Prefix, resp.NextContinuationToken,
	// 	len(resp.Contents), len(resp.CommonPrefixes), app.inputConcurrent)
	if len(resp.Contents) > 0 {
		cho.Push(status.Complete{Todo: resp.Contents, Completed: false})
	}
	if atomic.CompareAndSwapInt32(&app.InputConcurrent, 0, 0) {
		// fmt.Fprintln(os.Stderr, "Stop-Concurrent")
		cho.Push(status.Complete{Todo: nil, Completed: true})
	}
}

func DelimiterStrategy(app *config.S3StreamingLister, prefix *string, next *string, chi myq.MyQueue) {
	atomic.AddInt64(&app.Clients.Calls.Concurrent.ListObjectsV2Input, 1)
	chi.Push(&s3.ListObjectsV2Input{
		MaxKeys:           int32(*app.Config.MaxKeys),
		Delimiter:         app.Config.Delimiter,
		Prefix:            prefix,
		ContinuationToken: next,
		Bucket:            app.Config.Bucket,
	})
}

func SingleLetterStrategy(app *config.S3StreamingLister, prefix *string, chi myq.MyQueue) {
	for _, letter := range *app.Config.Prefixes {
		nextPrefix := *prefix + letter
		atomic.AddInt64(&app.Clients.Calls.Concurrent.ListObjectsV2Input, 1)
		chi.Push(&s3.ListObjectsV2Input{
			MaxKeys:   int32(*app.Config.MaxKeys),
			Delimiter: app.Config.Delimiter,
			Prefix:    &nextPrefix,
			Bucket:    app.Config.Bucket,
		})
	}
}

func S3ListerWorker(app *config.S3StreamingLister, cho myq.MyQueue, chstatus myq.MyQueue) myq.MyQueue {
	chi := myq.MakeChannelQueue((*app.Config.MaxKeys) * *app.Config.S3Workers)
	pooli := pond.New(*app.Config.S3Workers, len(*app.Config.Prefixes)**app.Config.MaxKeys*(*app.Config.S3Workers))
	go func() {
		chi.Wait(func(item interface{}) {
			citem := *item.(*s3.ListObjectsV2Input)
			pooli.Submit(func() {
				S3Lister(app, citem, chi, cho, chstatus)
			})
		})
	}()
	return chi
}
