package frontend

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/alitto/pond"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	config "github.com/mabels/s3-streaming-lister/config"
	myq "github.com/mabels/s3-streaming-lister/my-queue"
	"github.com/mabels/s3-streaming-lister/status"
)

func getClient(app *config.S3StreamingLister) *s3.Client {
	var client *s3.Client
	app.Clients.Calls.Concurrent.Inc("NewFromConfig")
	select {
	case x := <-app.Clients.Channels:
		client = x
	default:
		//fmt.Fprintln(os.Stderr, app.Config.listObject.aws.cfg)
		{
			defer app.Clients.Calls.Total.Duration("NewFromConfig", time.Now())
			client = s3.NewFromConfig(app.Config.ListObject.Aws.Cfg)
		}
	}
	// fmt.Fprintln(os.Stderr, "Pre=", *input.Prefix)
	app.Clients.Calls.Concurrent.Dec("NewFromConfig")
	return client
}

func S3Lister(app *config.S3StreamingLister, input s3.ListObjectsV2Input, chi myq.MyQueue, cho myq.MyQueue, chstatus myq.MyQueue) {
	client := getClient(app)

	app.Clients.Calls.Concurrent.Dec("ListObjectsV2Input")
	var resp *s3.ListObjectsV2Output
	var err error
	{
		app.Clients.Calls.Concurrent.Inc("ListObjectsV2")
		defer app.Clients.Calls.Total.Duration("ListObjectsV2", time.Now())
		resp, err = client.ListObjectsV2(context.TODO(), &input)
		app.Clients.Calls.Concurrent.Dec("ListObjectsV2")
	}
	app.Clients.Channels <- client

	if err != nil {
		app.Clients.Calls.Error.Inc("ListObjectsV2")
		chstatus.Push(status.RunStatus{Err: &err})
		// fmt.Fprintf(os.Stderr, "Got error retrieving list of objects:%s", *input.Bucket)
		// fmt.Fprintln(os.Stderr, err)
		return
	}
	if resp.NextContinuationToken != nil {
		// fmt.Fprintf(os.Stderr, "NextToken=%s\n", *input.Prefix)
		if *app.Config.Strategy == "delimiter" {
			app.Clients.Calls.Total.Inc("ListObjectsV2Input")
			// inputConcurrent not need for paging
			DelimiterStrategy(app, input.Prefix, resp.NextContinuationToken, chi)
		} else if *app.Config.Strategy == "letter" {
			// inputConcurrent is aborted
			atomic.AddInt32(&app.InputConcurrent, -1)
			// inputConcurrent next prefix is issued
			atomic.AddInt32(&app.InputConcurrent, int32(len(*app.Config.Prefixes)))
			SingleLetterStrategy(app, input.Prefix, chi)
			return
		}
	} else {
		// inputConcurrent end reached of paging
		atomic.AddInt32(&app.InputConcurrent, -1)
	}

	for _, item := range resp.CommonPrefixes {
		if *app.Config.Strategy == "delimiter" {
			atomic.AddInt32(&app.InputConcurrent, 1)
			DelimiterStrategy(app, item.Prefix, nil, chi)
		} else if *app.Config.Strategy == "letter" {
			out, _ := json.Marshal(resp.CommonPrefixes)
			err := fmt.Errorf("letter should not go to this:%s", string(out))
			chstatus.Push(status.RunStatus{Err: &err, Completed: true})
			return
		}
	}
	// fmt.Fprintln(os.Stderr, "Post=", *input.Prefix, resp.NextContinuationToken,
	// 	len(resp.Contents), len(resp.CommonPrefixes), app.inputConcurrent)
	if len(resp.Contents) > 0 {
		cho.Push(Complete{Todo: resp.Contents, Completed: false})
	}
	if atomic.CompareAndSwapInt32(&app.InputConcurrent, 0, 0) {
		// fmt.Fprintln(os.Stderr, "Stop-Concurrent")
		cho.Push(Complete{Todo: nil, Completed: true})
	}
}

func DelimiterStrategy(app *config.S3StreamingLister, prefix *string, next *string, chi myq.MyQueue) {
	app.Clients.Calls.Concurrent.Inc("ListObjectsV2Input")
	app.Clients.Calls.Total.Inc("ListObjectsV2Input")
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
		app.Clients.Calls.Total.Inc("ListObjectsV2Input")
		app.Clients.Calls.Concurrent.Inc("ListObjectsV2Input")
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
