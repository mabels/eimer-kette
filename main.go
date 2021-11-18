package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"sync/atomic"

	"github.com/alitto/pond"
	"github.com/spf13/cobra"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type Config struct {
	gitCommit     string
	version       string
	prefix        *string
	delimiter     *string
	format        *string
	bucket        *string
	region        *string
	maxKeys       *int32
	s3Workers     *int
	outWorkers    *int
	statsFragment *uint64
}

type Calls struct {
	newFromConfig      int64
	listObjectsV2      int64
	listObjectsV2Input int64
}

type TotalCurrent struct {
	total      Calls
	concurrent Calls
}

type Channels struct {
	calls    TotalCurrent
	channels chan *s3.Client
}

type Output struct {
	fileStream io.Writer
}

type S3StreamingLister struct {
	config          Config
	inputConcurrent int32
	clients         Channels
	output          Output
	aws             aws.Config
}

type RunStatus struct {
	outObjects uint64
}

type Complete struct {
	completed bool
	todo      []types.Object
}

func outWriter(app S3StreamingLister, tos Complete, chstatus chan RunStatus) {
	chstatus <- RunStatus{outObjects: uint64(len(tos.todo))}
	for _, item := range tos.todo {
		if *app.config.format == "mjson" {
			// Add BucketName
			// app.config.bucket
			out, _ := json.Marshal(item)
			fmt.Fprintln(app.output.fileStream, string(out))
		} else if *app.config.format == "sqs" {
			panic("not yet sqs")
			// out, _ := json.Marshal(item)
		}
	}
}

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
		fmt.Println("Got error retrieving list of objects:")
		fmt.Println(err)
		return
	}
	app.clients.channels <- client
	atomic.AddInt64(&app.clients.calls.concurrent.listObjectsV2Input, -1)
	atomic.AddInt64(&app.clients.calls.concurrent.listObjectsV2, -1)

	if resp.NextContinuationToken != nil {
		atomic.AddInt64(&app.clients.calls.total.listObjectsV2Input, 1)
		atomic.AddInt64(&app.clients.calls.concurrent.listObjectsV2Input, 1)
		chi <- &s3.ListObjectsV2Input{
			MaxKeys:           *app.config.maxKeys,
			Delimiter:         input.Delimiter,
			Prefix:            input.Prefix,
			ContinuationToken: resp.NextContinuationToken,
			Bucket:            input.Bucket,
		}
	} else {
		atomic.AddInt32(&app.inputConcurrent, -1)
	}

	atomic.AddInt32(&app.inputConcurrent, int32(len(resp.CommonPrefixes)))
	atomic.AddInt64(&app.clients.calls.total.listObjectsV2Input, int64(len(resp.CommonPrefixes)))
	atomic.AddInt64(&app.clients.calls.concurrent.listObjectsV2Input, int64(len(resp.CommonPrefixes)))
	for _, item := range resp.CommonPrefixes {
		chi <- &s3.ListObjectsV2Input{
			MaxKeys:   *app.config.maxKeys,
			Delimiter: app.config.delimiter,
			Prefix:    item.Prefix,
			Bucket:    app.config.bucket,
		}
	}
	cho <- Complete{todo: resp.Contents, completed: false}
	if atomic.CompareAndSwapInt32(&app.inputConcurrent, 0, 0) {
		fmt.Fprintln(os.Stderr, "Stop-Concurrent")
		cho <- Complete{todo: nil, completed: true}
	}
}

func versionStr(args *Config) string {
	return fmt.Sprintf("Version: %s:%s\n", args.version, args.gitCommit)
}

func parseArgs(app *S3StreamingLister, osArgs []string) {
	rootCmd := &cobra.Command{
		Use:     path.Base(osArgs[0]),
		Short:   "s3-streaming-lister short help",
		Long:    strings.TrimSpace("s3-streaming-lister long help"),
		Version: versionStr(&app.config),
		Args:    cobra.MinimumNArgs(0),
		// RunE:         S3StreamingListerCmdE(&app.config),
		SilenceUsage: true,
	}
	flags := rootCmd.PersistentFlags()
	app.config.prefix = flags.String("prefix", *app.config.prefix, "aws prefix")
	app.config.delimiter = flags.String("delimiter", *app.config.delimiter, "aws delimiter")
	app.config.format = flags.String("format", *app.config.format, "mjson | sqs")
	app.config.bucket = flags.String("bucket", *app.config.bucket, "aws bucket name")
	app.config.region = flags.String("region", *app.config.region, "aws region name")
	app.config.maxKeys = flags.Int32("maxKeys", *app.config.maxKeys, "aws maxKey pageElement size 1000")
	app.config.s3Workers = flags.Int("s3Worker", *app.config.s3Workers, "number of query worker")
	app.config.outWorkers = flags.Int("outWorkers", *app.config.outWorkers, "number of output worker")
	app.config.statsFragment = flags.Uint64("statsFragment", *app.config.statsFragment, "number statistics output")
	rootCmd.SetArgs(osArgs[1:])
	rootCmd.Execute()
}

func main() {
	prefix := ""
	delimiter := "/"
	mjson := "mjson"
	bucket := "unknown bucket"
	region := "eu-central-1"
	s3Workers := 16
	outWorkers := 1
	maxKeys := int32(1000)
	statsFragment := uint64(10000)
	app := S3StreamingLister{
		config: Config{
			prefix:        &prefix,
			delimiter:     &delimiter,
			format:        &mjson,
			bucket:        &bucket,
			region:        &region,
			s3Workers:     &s3Workers,
			outWorkers:    &outWorkers,
			maxKeys:       &maxKeys,
			statsFragment: &statsFragment,
		},
		inputConcurrent: 0,
		clients: Channels{
			calls: TotalCurrent{
				total: Calls{
					newFromConfig:      0,
					listObjectsV2:      0,
					listObjectsV2Input: 0,
				},
				concurrent: Calls{
					newFromConfig:      0,
					listObjectsV2:      0,
					listObjectsV2Input: 0,
				},
			},
			channels: make(chan *s3.Client, s3Workers),
		},
		aws: *aws.NewConfig(),
		output: Output{
			fileStream: os.Stdout,
		},
	}

	parseArgs(&app, os.Args)
	awsCfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(*app.config.region))
	if err != nil {
		panic("aws configuration error, " + err.Error())
	}
	app.aws = awsCfg

	chstatus := make(chan RunStatus, 100)
	go func() {
		total := uint64(0)
		lastTotal := uint64(0)
		for item := range chstatus {
			total += item.outObjects
			if lastTotal/(*app.config.statsFragment) != total/(*app.config.statsFragment) {
				fmt.Fprintf(os.Stderr, "Done=%d inputConcurrent=%d listObjectsV2=%d/%d listObjectsV2Input=%d/%d NewFromConfig=%d/%d\n",
					total,
					app.inputConcurrent,
					app.clients.calls.total.listObjectsV2,
					app.clients.calls.concurrent.listObjectsV2,
					app.clients.calls.total.listObjectsV2Input,
					app.clients.calls.concurrent.listObjectsV2Input,
					app.clients.calls.total.newFromConfig,
					app.clients.calls.concurrent.newFromConfig,
				)
				lastTotal = total
			}
		}
	}()

	cho := make(chan Complete, *app.config.maxKeys)
	chi := make(chan *s3.ListObjectsV2Input, (*app.config.maxKeys)*int32(*app.config.s3Workers))
	pooli := pond.New(*app.config.s3Workers, *app.config.s3Workers)
	go func() {
		for item := range chi {
			citem := *item
			pooli.Submit(func() {
				s3Lister(citem, chi, cho, &app)
			})
		}
	}()
	atomic.AddInt32(&app.inputConcurrent, 1)
	chi <- &s3.ListObjectsV2Input{
		MaxKeys:   *app.config.maxKeys,
		Delimiter: app.config.delimiter,
		Prefix:    app.config.prefix,
		Bucket:    app.config.bucket,
	}
	poolo := pond.New(*app.config.outWorkers, 0, pond.MinWorkers(*app.config.outWorkers))
	poolo.SubmitAndWait(func() {
		for items := range cho {
			if items.completed {
				fmt.Fprintln(os.Stderr, "Exit-Items", items)
				return
			}
			outWriter(app, items, chstatus)
		}
	})
	fmt.Fprintln(os.Stderr, "Exit")
}
