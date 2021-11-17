// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX - License - Identifier: Apache - 2.0
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sync/atomic"

	// "github.com/alitto/pond"

	"github.com/gammazero/workerpool"
	// "github.com/alitto/pond"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// S3ListObjectsAPI defines the interface for the ListObjectsV2 function.
// We use this interface to test the function using a mocked service.
// type S3ListObjectsAPI interface {
// ListObjectsV2(ctx context.Context,
// params *s3.ListObjectsV2Input,
// optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
// }

// GetObjects retrieves the objects in an Amazon Simple Storage Service (Amazon S3) bucket
// Inputs:
//     c is the context of the method call, which includes the AWS Region
//     api is the interface that defines the method call
//     input defines the input arguments to the service call.
// Output:
//     If success, a ListObjectsV2Output object containing the result of the service call and nil
//     Otherwise, nil and an error from the call to ListObjectsV2
// func GetObjects(c context.Context, api S3ListObjectsAPI, input *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
// }

type Config struct {
	prefix    string
	delimiter string
	bucket    string
	aws       aws.Config
	maxKeys   int32
	s3Worker  int
	outWorker int
}

type Calls struct {
	newFromConfig uint
	listObjectsV2 uint
}

type Channels struct {
	calls    Calls
	channels chan *s3.Client
}

type S3StreamingLister struct {
	config          Config
	inputConcurrent int32
	clients         Channels
}

type RunStatus struct {
	outObjects uint64
}

func outWriter(tos Complete, chstatus chan RunStatus) {
	chstatus <- RunStatus{outObjects: uint64(len(tos.todo))}
	for _, item := range tos.todo {
		out, _ := json.Marshal(item)
		fmt.Println(string(out))
	}
}

func s3Lister(input s3.ListObjectsV2Input, chi chan *s3.ListObjectsV2Input, cho chan Complete, app *S3StreamingLister) {
	var client *s3.Client
	// newFromConfig := false
	select {
	case x := <-app.clients.channels:
		client = x
	default:
		// newFromConfig = true
		app.clients.calls.newFromConfig += 1
		client = s3.NewFromConfig(app.config.aws)
	}
	// chx := observable.Observe()
	// fmt.Println("hallo")
	// input := item.V.(*s3.ListObjectsV2Input)
	// if input.Prefix != nil {
	// 	fmt.Fprintln(os.Stderr, "input bucket=", *input.Bucket, " newFromConfig=", newFromConfig, " next=", input.ContinuationToken, " prefix=", *input.Prefix)
	// } else {
	// 	fmt.Fprintln(os.Stderr, "input bucket=", *input.Bucket, " newFromConfig=", newFromConfig, " next=", input.ContinuationToken)
	// }
	// fmt.Fprintln(os.Stderr, "client=", client)
	app.clients.calls.listObjectsV2 += 1
	resp, err := client.ListObjectsV2(context.TODO(), &input)
	if err != nil {
		fmt.Println("Got error retrieving list of objects:")
		fmt.Println(err)
		return
	}
	// fmt.Fprintln(os.Stderr, "-- list objects v2")
	app.clients.channels <- client
	// fmt.Fprintln(os.Stderr, "-- pushed back to clients")
	// paging
	if resp.NextContinuationToken != nil {
		// fmt.Println("next", resp.NextContinuationToken)
		// go func() {
		// fmt.Fprintln(os.Stderr, "-- pre next")
		// fmt.Fprintln(os.Stderr, "Prefix=", *input.Prefix, " next=", resp.NextContinuationToken)
		chi <- &s3.ListObjectsV2Input{
			MaxKeys:           app.config.maxKeys,
			Delimiter:         input.Delimiter,
			Prefix:            input.Prefix,
			ContinuationToken: resp.NextContinuationToken,
			Bucket:            input.Bucket,
		}
		// fmt.Fprintln(os.Stderr, "-- post next")
		// }()
	} else {
		atomic.AddInt32(&app.inputConcurrent, -1)
	}
	// out := ""
	// if input.Prefix != nil {
	// out = *input.Prefix
	// }
	// fmt.Println("prefix=", out, "len=", len(resp.Contents), " next=", resp.NextContinuationToken, " res=", *resp.Contents[0].Key)
	// sub prefixe
	atomic.AddInt32(&app.inputConcurrent, int32(len(resp.CommonPrefixes)))
	// go func() {
	// fmt.Fprintln(os.Stderr, "-- pre prefixed=%d", len(resp.CommonPrefixes))
	for _, item := range resp.CommonPrefixes {
		// fmt.Fprintln(os.Stderr, "Prefix=", *item.Prefix)
		chi <- &s3.ListObjectsV2Input{
			MaxKeys:   app.config.maxKeys,
			Delimiter: &app.config.delimiter,
			Prefix:    item.Prefix,
			Bucket:    &app.config.bucket,
		}
	}
	// fmt.Fprintln(os.Stderr, "-- post prefixed contents=%d", len(resp.Contents))
	// }()
	// output of files
	// go func() {
	// fmt.Fprintln(os.Stderr, "Contents:", len(resp.Contents))
	cho <- Complete{todo: resp.Contents, completed: false}
	// fmt.Fprintln(os.Stderr, "-- post contents")
	if app.inputConcurrent == 0 {
		fmt.Fprintln(os.Stderr, "Stop-Concurrent")
		cho <- Complete{todo: nil, completed: true}
	}
	// }()

	// fmt.Println("Objects in " + *bucket)
	// for _, item := range resp.Contents {
	// 	fmt.Println("Name:          ", *item.Key)
	// 	//	fmt.Println("Last modified: ", *item.LastModified)
	// 	//	fmt.Println("Size:          ", item.Size)
	// 	//	fmt.Println("Storage class: ", item.StorageClass)
	// 	//	fmt.Println("")
	// }

	// fmt.Println("Found", len(resp.Contents), "items in bucket", *bucket, " next=", resp.NextContinuationToken)
	// fmt.Println("")
}

type Complete struct {
	completed bool
	todo      []types.Object
}

func main() {
	bucket := flag.String("b", "", "The name of the bucket")
	flag.Parse()

	if *bucket == "" {
		fmt.Println("You must supply the name of a bucket (-b BUCKET)")
		return
	}

	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		panic("configuration error, " + err.Error())
	}

	// observable := rxgo.Defer([]rxgo.Producer{func(_ context.Context, ch chan<- rxgo.Item) {
	// 	for i := 0; i < 3; i++ {
	// 		ch <- rxgo.Of(i)
	// 	}
	// }})

	// Submit 1000 tasks
	// for i := 0; i < 1000; i++ {
	// n := i
	// pool.Submit(func() {
	// fmt.Printf("Running task #%d\n", n)
	// })
	// }

	// Stop the pool and wait for all submitted tasks to complete
	// pool.StopAndWait()
	// cho := make(chan rxgo.Item)

	s3Worker := 16
	app := S3StreamingLister{
		config: Config{
			prefix:    "",
			delimiter: "/",
			bucket:    *bucket,
			aws:       cfg,
			s3Worker:  s3Worker,
			outWorker: 1,
			maxKeys:   1000,
		},
		inputConcurrent: 0,
		clients: Channels{
			calls: Calls{
				newFromConfig: 0,
				listObjectsV2: 0,
			},
			channels: make(chan *s3.Client, s3Worker),
		},
	}

	chstatus := make(chan RunStatus, 100)
	go func() {
		total := uint64(0)
		lastTotal := uint64(0)
		for item := range chstatus {
			total += item.outObjects
			if lastTotal/2000 != total/2000 {
				fmt.Fprintf(os.Stderr, "Done=%d ListObjects=%d NewFromConfig=%d\n", total, app.clients.calls.listObjectsV2, app.clients.calls.newFromConfig)
				lastTotal = total
			}
		}
	}()

	cho := make(chan Complete, 100)
	chi := make(chan *s3.ListObjectsV2Input, 2000)
	// pooli := pond.New(app.config.s3Worker, app.config.s3Worker) //, pond.MinWorkers(app.config.s3Worker))
	pooli := workerpool.New(app.config.s3Worker)
	go func() {
		for item := range chi {
			// fmt.Fprintln(os.Stderr, "FromChi prefix=", *item.Prefix, " item=", item)
			citem := *item
			pooli.Submit(func() {
				// fmt.Fprintln(os.Stderr, "SubmitChi prefix=", *citem.Prefix, " item=", citem)
				s3Lister(citem, chi, cho, &app)
			})
		}
		// fmt.Fprintln(os.Stderr, "Exit the Chi")
	}()
	// for i := 0; i < 8; i++ {
	// wgi.Add(i)
	// go s3Lister(&wgi, chi, cho, cfg, bucket, &delimiter, &prefix, &s3ListerConcurrent)
	// }
	atomic.AddInt32(&app.inputConcurrent, 1)
	chi <- &s3.ListObjectsV2Input{
		MaxKeys:   app.config.maxKeys,
		Delimiter: &app.config.delimiter,
		Prefix:    &app.config.prefix,
		Bucket:    &app.config.bucket,
	}
	// poolo := pond.New(app.config.outWorker, 0, pond.MinWorkers(app.config.outWorker))
	poolo := workerpool.New(app.config.outWorker)
	poolo.SubmitWait(func() {
		for items := range cho {
			if items.completed {
				fmt.Fprintln(os.Stderr, "Exit-Items", items)
				return
			}
			// fmt.Fprintln(os.Stderr, "Items", items)
			outWriter(items, chstatus)
		}
	})
	fmt.Fprintln(os.Stderr, "Exit")

	// time.Sleep(time.Second * 10)

	// 	observable.w
	// disposed, cancel := observable.Connect()
	// go func() {
	// 	// Do something
	// 	time.Sleep(time.Second)
	// 	// Then cancel the subscription
	// 	cancel()
	// }()
	// // Wait for the subscription to be disposed
	// <-disposed
	// 	// }()

	// prefix := ""
	// delimiter := "/"
	// input := &s3.ListObjectsV2Input{
	// 	Delimiter: &delimiter,
	// 	Prefix:    &prefix,
	// 	Bucket:    bucket,
	// }

}
