package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync/atomic"

	"github.com/alitto/pond"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func s3Lister(app *S3StreamingLister, input s3.ListObjectsV2Input, chi Queue, cho Queue, chstatus Queue) {
	var client *s3.Client
	atomic.AddInt64(&app.clients.calls.concurrent.newFromConfig, 1)
	select {
	case x := <-app.clients.channels:
		client = x
	default:
		atomic.AddInt64(&app.clients.calls.total.newFromConfig, 1)
		//fmt.Fprintln(os.Stderr, app.config.listObject.aws.cfg)
		client = s3.NewFromConfig(app.config.listObject.aws.cfg)
	}
	// fmt.Fprintln(os.Stderr, "Pre=", *input.Prefix)
	atomic.AddInt64(&app.clients.calls.concurrent.newFromConfig, -1)
	atomic.AddInt64(&app.clients.calls.total.listObjectsV2, 1)
	atomic.AddInt64(&app.clients.calls.concurrent.listObjectsV2, 1)
	resp, err := client.ListObjectsV2(context.TODO(), &input)
	if err != nil {
		chstatus.push(RunStatus{err: &err})
		// fmt.Fprintf(os.Stderr, "Got error retrieving list of objects:%s", *input.Bucket)
		// fmt.Fprintln(os.Stderr, err)
		return
	}
	app.clients.channels <- client
	atomic.AddInt64(&app.clients.calls.concurrent.listObjectsV2Input, -1)
	atomic.AddInt64(&app.clients.calls.concurrent.listObjectsV2, -1)

	if resp.NextContinuationToken != nil {
		atomic.AddInt64(&app.clients.calls.total.listObjectsV2Input, 1)
		atomic.AddInt64(&app.clients.calls.concurrent.listObjectsV2Input, 1)
		if *app.config.strategy == "delimiter" {
			delimiterStrategy(&app.config, input.Prefix, resp.NextContinuationToken, chi)
		} else if *app.config.strategy == "letter" {
			atomic.AddInt32(&app.inputConcurrent, -1)
			atomic.AddInt32(&app.inputConcurrent, int32(len(*app.config.prefixes)))
			singleLetterStrategy(&app.config, input.Prefix, chi)
			return
		}
	} else {
		atomic.AddInt32(&app.inputConcurrent, -1)
	}

	atomic.AddInt64(&app.clients.calls.total.listObjectsV2Input, int64(len(resp.CommonPrefixes)))
	atomic.AddInt64(&app.clients.calls.concurrent.listObjectsV2Input, int64(len(resp.CommonPrefixes)))
	for _, item := range resp.CommonPrefixes {
		if *app.config.strategy == "delimiter" {
			atomic.AddInt32(&app.inputConcurrent, 1)
			delimiterStrategy(&app.config, item.Prefix, nil, chi)
		} else if *app.config.strategy == "letter" {
			out, _ := json.Marshal(resp.CommonPrefixes)
			fmt.Fprintln(os.Stderr, string(out))
			panic("letter should not go to this")
		}
	}
	// fmt.Fprintln(os.Stderr, "Post=", *input.Prefix, resp.NextContinuationToken,
	// 	len(resp.Contents), len(resp.CommonPrefixes), app.inputConcurrent)
	cho.push(Complete{todo: resp.Contents, completed: false})
	if atomic.CompareAndSwapInt32(&app.inputConcurrent, 0, 0) {
		// fmt.Fprintln(os.Stderr, "Stop-Concurrent")
		cho.push(Complete{todo: nil, completed: true})
	}
}

func delimiterStrategy(config *Config, prefix *string, next *string, chi Queue) {
	chi.push(&s3.ListObjectsV2Input{
		MaxKeys:           int32(*config.maxKeys),
		Delimiter:         config.delimiter,
		Prefix:            prefix,
		ContinuationToken: next,
		Bucket:            config.bucket,
	})
}

func singleLetterStrategy(config *Config, prefix *string, chi Queue) {
	for _, letter := range *config.prefixes {
		nextPrefix := *prefix + letter
		chi.push(&s3.ListObjectsV2Input{
			MaxKeys:   int32(*config.maxKeys),
			Delimiter: config.delimiter,
			Prefix:    &nextPrefix,
			Bucket:    config.bucket,
		})
	}
}

func s3ListerWorker(app *S3StreamingLister, cho Queue, chstatus Queue) Queue {
	chi := makeChannelQueue((*app.config.maxKeys) * *app.config.s3Workers)
	pooli := pond.New(*app.config.s3Workers, *app.config.maxKeys*(*app.config.s3Workers))
	go func() {
		chi.wait(func(item interface{}) {
			citem := *item.(*s3.ListObjectsV2Input)
			pooli.Submit(func() {
				s3Lister(app, citem, chi, cho, chstatus)
			})
		})
	}()
	return chi
}
