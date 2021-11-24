package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"strings"

	"github.com/spf13/cobra"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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
	maxKeys       *int
	s3Workers     *int
	outWorkers    *int
	statsFragment *uint64
	help          bool
	versionFlag   bool
	progress      *bool
	outputSqs     SqsParams
}

type SqsParams struct {
	url            *string
	delay          *int32
	maxMessageSize *int32
}

type Calls struct {
	newFromConfig      int64
	listObjectsV2      int64
	listObjectsV2Input int64
	newSqs             int64
	sqsSendMessage     int64
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

var GitCommit string
var Version string

func defaultS3StreamingLister() *S3StreamingLister {
	prefix := ""
	delimiter := "/"
	mjson := "mjson"
	outputSqsUrl := ""
	outputSqsDelay := int32(10)
	outputSqsMaxMessageSize := int32(137715)
	// bucket := nil
	region := "eu-central-1"
	s3Workers := 16
	outWorkers := 1
	maxKeys := 1000
	statsFragment := uint64(10000)
	// allowed characters: https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
	prefixes := []string{
		"0", "1", "2", "3", "4", "5", "6", "7", "8", "9",
		"A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M",
		"N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z",
		"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m",
		"n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z",
		"/", "!", "-", "_", ".", "*", "'", "(", ")",
	}
	strategie := "delimiter"
	progress := true
	app := S3StreamingLister{
		config: Config{
			version:   Version,
			gitCommit: GitCommit,
			prefix:    &prefix,
			delimiter: &delimiter,
			strategie: &strategie,
			prefixes:  &prefixes,
			format:    &mjson,
			outputSqs: SqsParams{
				delay:          &outputSqsDelay,
				url:            &outputSqsUrl,
				maxMessageSize: &outputSqsMaxMessageSize,
			},
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
	return &app
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
	app.config.prefixes = flags.StringArray("prefixes", *app.config.prefixes, "prefixs (safe characters are default, see https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html )")
	app.config.prefix = flags.String("prefix", *app.config.prefix, "aws prefix")
	app.config.delimiter = flags.String("delimiter", *app.config.delimiter, "aws delimiter")
	app.config.format = flags.String("format", *app.config.format, "mjson | sqs | awsls")
	app.config.outputSqs.url = flags.String("outputSqsUrl", *app.config.outputSqs.url, "url")
	app.config.outputSqs.delay = flags.Int32("outputSqsDelay", *app.config.outputSqs.delay, "delay")
	app.config.outputSqs.maxMessageSize = flags.Int32("outputSqsMaxMessageSize", *app.config.outputSqs.maxMessageSize, "maxMessageSize")
	app.config.bucket = flags.StringP("bucket", "b", "", "aws bucket name")
	app.config.region = flags.String("region", *app.config.region, "aws region name")
	app.config.maxKeys = flags.Int("maxKeys", *app.config.maxKeys, "aws maxKey pageElement size 1000")
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

func initS3StreamingLister(app *S3StreamingLister) {
	err := parseArgs(app, os.Args)
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
}
