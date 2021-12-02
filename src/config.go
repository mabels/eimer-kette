package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type Config struct {
	gitCommit     string
	version       string
	strategy      *string
	prefixes      *[]string
	prefix        *string
	delimiter     *string
	format        *string
	bucket        *string
	maxKeys       *int
	s3Workers     *int
	statsFragment *uint64
	help          bool
	versionFlag   bool
	progress      *bool
	outputSqs     SqsParams
	outputSqlite  SqliteParams
	listObject    ListObjectParams
	lambda        LambdaParams
}

type AwsParams struct {
	keyId           *string
	secretAccessKey *string
	sessionToken    *string
	region          *string
	cfg             aws.Config
}

type LambdaParams struct {
	deploy *bool
	start  *bool
	aws    AwsParams
}

type SqsParams struct {
	workers        *int
	url            *string
	delay          *int32
	maxMessageSize *int32
	aws            AwsParams
}

type SqliteParams struct {
	workers  *int
	cleanDb  *bool
	filename *string
	sqlTable *string
	// url            *string
	// delay          *int32
	// maxMessageSize *int32
	// aws            AwsParams
}

type ListObjectParams struct {
	aws AwsParams
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
	keyId := ""
	secretAccessKey := ""
	sessionToken := ""
	region := "eu-central-1"
	s3Workers := 16
	sqlWorkers := 8
	outputSqsWorkers := 2
	maxKeys := 1000
	statsFragment := uint64(10000)
	falseVal := false
	sqlTables := ""
	// allowed characters: https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
	prefixes := []string{
		"0", "1", "2", "3", "4", "5", "6", "7", "8", "9",
		"A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M",
		"N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z",
		"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m",
		"n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z",
		"/", "!", "-", "_", ".", "*", "'", "(", ")",
	}
	strategy := "delimiter"
	progress := true
	sqlFilename := "./file.sql"
	app := S3StreamingLister{
		config: Config{
			version:   Version,
			gitCommit: GitCommit,
			prefix:    &prefix,
			delimiter: &delimiter,
			strategy:  &strategy,
			prefixes:  &prefixes,
			format:    &mjson,
			outputSqlite: SqliteParams{
				workers:  &sqlWorkers,
				cleanDb:  &falseVal,
				filename: &sqlFilename,
				sqlTable: &sqlTables,
			},
			outputSqs: SqsParams{
				workers:        &outputSqsWorkers,
				delay:          &outputSqsDelay,
				url:            &outputSqsUrl,
				maxMessageSize: &outputSqsMaxMessageSize,
				aws: AwsParams{
					keyId:           &keyId,
					secretAccessKey: &secretAccessKey,
					sessionToken:    &sessionToken,
					region:          &region,
				},
			},
			lambda: LambdaParams{
				deploy: &falseVal,
				start:  &falseVal,
				aws: AwsParams{
					keyId:           &keyId,
					secretAccessKey: &secretAccessKey,
					sessionToken:    &sessionToken,
					region:          &region,
				},
			},
			bucket:        nil,
			s3Workers:     &s3Workers,
			maxKeys:       &maxKeys,
			statsFragment: &statsFragment,
			progress:      &progress,
			listObject: ListObjectParams{
				aws: AwsParams{
					keyId:           &keyId,
					secretAccessKey: &secretAccessKey,
					sessionToken:    &sessionToken,
					region:          &region,
				},
			},
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
		output: Output{
			fileStream: os.Stdout,
			// aws:        *aws.NewConfig(),
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
	app.config.strategy = flags.String("strategy", *app.config.strategy, "delimiter | letter")
	app.config.prefixes = flags.StringArray("prefixes", *app.config.prefixes, "prefixs (safe characters are default, see https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html )")
	app.config.prefix = flags.String("prefix", *app.config.prefix, "aws prefix")
	app.config.delimiter = flags.String("delimiter", *app.config.delimiter, "aws delimiter")
	app.config.format = flags.String("format", *app.config.format, "mjson | sqs | awsls")
	app.config.outputSqs.url = flags.String("outputSqsUrl", *app.config.outputSqs.url, "url")
	app.config.outputSqs.delay = flags.Int32("outputSqsDelay", *app.config.outputSqs.delay, "delay")
	app.config.outputSqs.maxMessageSize = flags.Int32("outputSqsMaxMessageSize", *app.config.outputSqs.maxMessageSize, "maxMessageSize")
	app.config.outputSqs.workers = flags.Int("outputSqsWorkers", *app.config.outputSqs.workers, "number of output sqs workers")
	app.config.bucket = flags.StringP("bucket", "b", "", "aws bucket name")
	app.config.maxKeys = flags.Int("maxKeys", *app.config.maxKeys, "aws maxKey pageElement size 1000")
	app.config.s3Workers = flags.Int("s3Worker", *app.config.s3Workers, "number of query workers")
	app.config.statsFragment = flags.Uint64("statsFragment", *app.config.statsFragment, "number statistics output")
	app.config.progress = flags.Bool("progress", *app.config.progress, "progress output")
	rootCmd.MarkFlagRequired("bucket")

	app.config.outputSqlite.cleanDb = flags.Bool("sqliteCleanDb", *app.config.outputSqlite.cleanDb, "set cleandb")
	app.config.outputSqlite.workers = flags.Int("sqliteWorkers", *app.config.outputSqlite.workers, "writer workers")
	app.config.outputSqlite.filename = flags.String("sqliteFilename", *app.config.outputSqlite.filename, "sqlite filename")
	app.config.outputSqlite.sqlTable = flags.String("sqliteTable", *app.config.outputSqlite.sqlTable, "sqlite table name")

	app.config.lambda.start = flags.Bool("lambdaStart", *app.config.lambda.start, "start lambda")
	app.config.lambda.deploy = flags.Bool("lambdaDeploy", *app.config.lambda.deploy, "deploy the lambda")

	flagsAws("lambda", flags, &app.config.lambda.aws)
	flagsAws("listObject", flags, &app.config.listObject.aws)
	flagsAws("outputSqs", flags, &app.config.outputSqs.aws)

	rootCmd.SetArgs(osArgs[1:])
	err := rootCmd.Execute()

	app.config.help = rootCmd.Flags().Lookup("help").Value.String() == "true"
	app.config.versionFlag = rootCmd.Flags().Lookup("version").Value.String() == "true"

	return err
}

func flagsAws(prefix string, flags *pflag.FlagSet, awsParams *AwsParams) {
	defaultAWSAccessKeyId := *awsParams.keyId
	defaultAWSSecretAccessKey := *awsParams.secretAccessKey
	defaultAWSSessionToken := *awsParams.sessionToken
	defaultAWSRegion := *awsParams.region
	prefixes := []string{"", strings.ToUpper(fmt.Sprintf("%s_", prefix))}
	envKeyIds := make([]string, len(prefixes))
	envSecretAccessKeys := make([]string, len(prefixes))
	envSessionTokens := make([]string, len(prefixes))
	envRegions := make([]string, len(prefixes))
	for i, p := range prefixes {
		envKeyIds[i] = fmt.Sprintf("%sAWS_ACCESS_KEY_ID", p)
		envSecretAccessKeys[i] = fmt.Sprintf("%sAWS_SECRET_ACCESS_KEY", p)
		envSessionTokens[i] = fmt.Sprintf("%sAWS_SESSION_TOKEN", p)
		envRegions[i] = fmt.Sprintf("%sAWS_REGION", p)
	}
	for i, _ := range prefixes {
		tmp, found := os.LookupEnv(envKeyIds[i])
		if found {
			defaultAWSAccessKeyId = tmp
		}
		tmp, found = os.LookupEnv(envSecretAccessKeys[i])
		if found {
			defaultAWSSecretAccessKey = tmp
		}
		tmp, found = os.LookupEnv(envSessionTokens[i])
		if found {
			defaultAWSSessionToken = tmp
		}
		tmp, found = os.LookupEnv(envRegions[i])
		if found {
			defaultAWSRegion = tmp
		}
	}
	awsParams.keyId = flags.String(fmt.Sprintf("%sAwsAccessKeyId", prefix), defaultAWSAccessKeyId, strings.Join(envKeyIds, ","))
	awsParams.secretAccessKey = flags.String(fmt.Sprintf("%sAwsSecretAccessKey", prefix), defaultAWSSecretAccessKey, strings.Join(envSecretAccessKeys, ","))
	awsParams.sessionToken = flags.String(fmt.Sprintf("%sAwsSessionToken", prefix), defaultAWSSessionToken, strings.Join(envSessionTokens, ","))
	awsParams.region = flags.String(fmt.Sprintf("%sAwsRegion", prefix), defaultAWSRegion, strings.Join(envRegions, ","))
}

type MyCredentials struct {
	cred aws.Credentials
}

func (my *MyCredentials) Retrieve(ctx context.Context) (aws.Credentials, error) {
	return my.cred, nil
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
	for _, awsParams := range []*AwsParams{
		&app.config.lambda.aws,
		&app.config.listObject.aws,
		&app.config.outputSqs.aws} {
		// awsConfig, err := config.LoadOptions(context.TODO()) // config.WithRegion(*app.config.region),

		// if err != nil {
		// 	panic("aws configuration error, " + err.Error())
		// }

		awsCredProvider := MyCredentials{
			cred: aws.Credentials{
				AccessKeyID:     *awsParams.keyId,
				SecretAccessKey: *awsParams.secretAccessKey,
				SessionToken:    *awsParams.sessionToken,
			},
		}
		// fmt.Fprintln(os.Stderr, "Region=", *awsParams.region)
		awsParams.cfg = aws.Config{
			Credentials: &awsCredProvider,
			// func (fn CredentialsProviderFunc) Retrieve(ctx context.Context) (Credentials, error) {
			// return fn(ctx)
			// },
			Region: *awsParams.region,
		}
	}
}
