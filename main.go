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
	strategie     *string
	prefixes      *[]string
	prefix        *string
	delimiter     *string
	format        *string
	bucket        *string
	region        *string
	maxKeys       *int32
	s3Workers     *int
	outWorkers    *int
	statsFragment *uint64
	help          bool
	versionFlag   bool
	progress      *bool
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
	completed  bool
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
	if tos.completed {
		// fmt.Fprintln(os.Stderr, "outWriter-Complete")
		chstatus <- RunStatus{outObjects: 0, completed: true}
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

func versionStr(args *Config) string {
	return fmt.Sprintf("Version: %s:%s\n", args.version, args.gitCommit)
}

func parseArgs(app *S3StreamingLister, osArgs []string) error {
	rootCmd := &cobra.Command{
		Use:     path.Base(osArgs[0]),
		Short:   "s3-streaming-lister short help",
		Long:    strings.TrimSpace("s3-streaming-lister long help"),
		Version: versionStr(&app.config),
		// Args:    cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return nil // errors.New("Provide item to the say command")
		},
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Fprintln(os.Stderr, "Hello World!")
		},
		SilenceUsage: true,
	}
	flags := rootCmd.Flags()
	app.config.strategie = flags.String("strategie", *app.config.strategie, "delimiter | letter")
	app.config.prefixes = flags.StringArray("prefixes", *app.config.prefixes, "prefixs")
	app.config.prefix = flags.String("prefix", *app.config.prefix, "aws prefix")
	app.config.delimiter = flags.String("delimiter", *app.config.delimiter, "aws delimiter")
	app.config.format = flags.String("format", *app.config.format, "mjson | sqs")
	app.config.bucket = flags.StringP("bucket", "b", "", "aws bucket name")
	app.config.region = flags.String("region", *app.config.region, "aws region name")
	app.config.maxKeys = flags.Int32("maxKeys", *app.config.maxKeys, "aws maxKey pageElement size 1000")
	app.config.s3Workers = flags.Int("s3Worker", *app.config.s3Workers, "number of query worker")
	app.config.outWorkers = flags.Int("outWorkers", *app.config.outWorkers, "number of output worker")
	app.config.statsFragment = flags.Uint64("statsFragment", *app.config.statsFragment, "number statistics output")
	app.config.progress = flags.Bool("progress", *app.config.progress, "progress output")
	rootCmd.MarkFlagRequired("bucket")
	// fmt.Fprintln(os.Stderr, string(out))
	rootCmd.SetArgs(osArgs[1:])
	err := rootCmd.Execute()
	// fmt.Fprintf(os.Stderr, "xxx:%s\n", rootCmd.Flags().Lookup("help").Value.String())
	app.config.help = rootCmd.Flags().Lookup("help").Value.String() == "true"
	app.config.versionFlag = rootCmd.Flags().Lookup("version").Value.String() == "true"
	return err
}

var GitCommit string
var Version string

func main() {
	prefix := ""
	delimiter := "/"
	mjson := "mjson"
	// bucket := nil
	region := "eu-central-1"
	s3Workers := 16
	outWorkers := 1
	maxKeys := int32(1000)
	statsFragment := uint64(10000)
	prefixes := []string{
		"A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M",
		"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m",
		"N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z",
		"n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z",
		"0", "1", "2", "3", "4", "5", "6", "7", "8", "9",
		"`", "!", "@", "#", "$", "%", "^", "&", "*", "(",
		")", "-", "_", "=", "+", "{", "}", "[", "]", "\\",
		"|", ":", ";", "\"", "'", "?", "/", ".", ">", ",", "<",
	}
	strategie := "delimiter"
	progress := true
	app := S3StreamingLister{
		config: Config{
			version:       Version,
			gitCommit:     GitCommit,
			prefix:        &prefix,
			delimiter:     &delimiter,
			strategie:     &strategie,
			prefixes:      &prefixes,
			format:        &mjson,
			bucket:        nil,
			region:        &region,
			s3Workers:     &s3Workers,
			outWorkers:    &outWorkers,
			maxKeys:       &maxKeys,
			statsFragment: &statsFragment,
			progress:      &progress,
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

	err := parseArgs(&app, os.Args)
	if err != nil {
		panic("cobra error, " + err.Error())
	}
	if app.config.help || app.config.versionFlag {
		os.Exit(0)
		return
	}
	// fmt.Fprintf(os.Stderr, "XXX:%p:%s", app.config.bucket, *app.config.bucket)
	if app.config.bucket == nil || len(*app.config.bucket) == 0 || app.config.help {
		os.Exit(1)
		return
	}
	awsCfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(*app.config.region))
	if err != nil {
		panic("aws configuration error, " + err.Error())
	}
	app.aws = awsCfg

	chstatus := make(chan RunStatus, 100)

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

	poolo := pond.New(*app.config.outWorkers, 0, pond.MinWorkers(*app.config.outWorkers))
	poolo.Submit(func() {
		for items := range cho {
			outWriter(app, items, chstatus)
			if items.completed {
				// fmt.Fprintln(os.Stderr, "Exit-Items", items)
				return
			}
		}
	})

	if *app.config.strategie == "delimiter" {
		atomic.AddInt32(&app.inputConcurrent, 1)
		delimiterStrategie(&app.config, app.config.prefix, nil, chi)
	} else if *app.config.strategie == "letter" {
		atomic.AddInt32(&app.inputConcurrent, int32(len(*app.config.prefixes)))
		singleLetterStrategie(&app.config, app.config.prefix, chi)
	}

	total := uint64(0)
	lastTotal := uint64(0)
	for item := range chstatus {
		total += item.outObjects
		if item.completed || lastTotal/(*app.config.statsFragment) != total/(*app.config.statsFragment) {
			if *app.config.progress {
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
			}
			lastTotal = total
			if item.completed {
				break
			}
		}
	}
	// fmt.Fprintln(os.Stderr, "Exit")
}
