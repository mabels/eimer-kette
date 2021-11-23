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

func s3Lister(input s3.ListObjectsV2Input, chi chan *s3.ListObjectsV2Input, cho chan Complete, app *S3StreamingLister) {
	var client *s3.Client
	atomic.AddInt64(&app.clients.calls.concurrent.newFromConfig, 1)
	select {
	case x := <-app.clients.channels:
		client = x
	default:
		atomic.AddInt64(&app.clients.calls.total.newFromConfig, 1)
		client = s3.NewFromConfig(app.aws)
	}
	atomic.AddInt64(&app.clients.calls.concurrent.newFromConfig, -1)
	atomic.AddInt64(&app.clients.calls.total.listObjectsV2, 1)
	atomic.AddInt64(&app.clients.calls.concurrent.listObjectsV2, 1)
	resp, err := client.ListObjectsV2(context.TODO(), &input)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Got error retrieving list of objects:%s", *input.Bucket)
		fmt.Fprintln(os.Stderr, err)
		return
	}
	app.clients.channels <- client
	atomic.AddInt64(&app.clients.calls.concurrent.listObjectsV2Input, -1)
	atomic.AddInt64(&app.clients.calls.concurrent.listObjectsV2, -1)

	if resp.NextContinuationToken != nil {
		atomic.AddInt64(&app.clients.calls.total.listObjectsV2Input, 1)
		atomic.AddInt64(&app.clients.calls.concurrent.listObjectsV2Input, 1)
		if *app.config.strategie == "delimiter" {
			delimiterStrategie(&app.config, input.Prefix, resp.NextContinuationToken, chi)
		} else if *app.config.strategie == "letter" {
			atomic.AddInt32(&app.inputConcurrent, -1)
			atomic.AddInt32(&app.inputConcurrent, int32(len(*app.config.prefixes)))
			singleLetterStrategie(&app.config, input.Prefix, chi)
			return
		}
	} else {
		atomic.AddInt32(&app.inputConcurrent, -1)
	}

	atomic.AddInt64(&app.clients.calls.total.listObjectsV2Input, int64(len(resp.CommonPrefixes)))
	atomic.AddInt64(&app.clients.calls.concurrent.listObjectsV2Input, int64(len(resp.CommonPrefixes)))
	for _, item := range resp.CommonPrefixes {
		if *app.config.strategie == "delimiter" {
			atomic.AddInt32(&app.inputConcurrent, 1)
			delimiterStrategie(&app.config, item.Prefix, nil, chi)
		} else if *app.config.strategie == "letter" {
			out, _ := json.Marshal(resp.CommonPrefixes)
			fmt.Fprintln(os.Stderr, string(out))
			panic("letter should not go to this")
		}
	}
	cho <- Complete{todo: resp.Contents, completed: false}
	if atomic.CompareAndSwapInt32(&app.inputConcurrent, 0, 0) {
		// fmt.Fprintln(os.Stderr, "Stop-Concurrent")
		cho <- Complete{todo: nil, completed: true}
	}
}

func delimiterStrategie(config *Config, prefix *string, next *string, chi chan *s3.ListObjectsV2Input) {
	chi <- &s3.ListObjectsV2Input{
		MaxKeys:           *config.maxKeys,
		Delimiter:         config.delimiter,
		Prefix:            prefix,
		ContinuationToken: next,
		Bucket:            config.bucket,
	}
}

func singleLetterStrategie(config *Config, prefix *string, chi chan *s3.ListObjectsV2Input) {
	for _, letter := range *config.prefixes {
		nextPrefix := *prefix + letter
		chi <- &s3.ListObjectsV2Input{
			MaxKeys:   *config.maxKeys,
			Delimiter: config.delimiter,
			Prefix:    &nextPrefix,
			Bucket:    config.bucket,
		}
	}
}

func s3ListerWorker(app *S3StreamingLister, cho chan Complete) chan *s3.ListObjectsV2Input {
	chi := make(chan *s3.ListObjectsV2Input, (*app.config.maxKeys)*int32(*app.config.s3Workers))
	pooli := pond.New(*app.config.s3Workers, *app.config.s3Workers)
	go func() {
		for item := range chi {
			citem := *item
			pooli.Submit(func() {
				s3Lister(citem, chi, cho, app)
			})
		}
	}()
	return chi
}
