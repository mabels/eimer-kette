package config

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
	GitCommit     string
	Version       string
	Strategy      *string
	Prefixes      *[]string
	Prefix        *string
	Delimiter     *string
	Format        *string
	Bucket        *string
	MaxKeys       *int
	S3Workers     *int
	StatsFragment *uint64
	Help          bool
	VersionFlag   bool
	Progress      *bool
	Output        OutputParams
	ListObject    ListObjectParams
	Lambda        LambdaParams
	Frontend      FrontendParams
}

type FrontendParams struct {
	Sqlite   SqliteFrontendParams
	Frontend *string
}

type SqliteFrontendParams struct {
	Filename  *string
	Query     *string
	TableName *string
}

type OutputParams struct {
	Sqs      SqsParams
	Sqlite   SqliteParams
	S3Delete S3DeleteParams
	DynamoDb DynamoDbParams
}

type AwsParams struct {
	KeyId           *string
	SecretAccessKey *string
	SessionToken    *string
	Region          *string
	Cfg             aws.Config
}

type LambdaParams struct {
	Deploy *bool
	Start  *bool
	Aws    AwsParams
}

type SqsParams struct {
	Workers        *int
	ChunkSize      *int
	Url            *string
	Delay          *int32
	MaxMessageSize *int
	Aws            AwsParams
}

type S3DeleteParams struct {
	Workers   *int
	ChunkSize *int
	Aws       AwsParams
}

type DynamoDbParams struct {
	Workers *int
	Aws     AwsParams
}

type SqliteParams struct {
	Workers    *int
	CommitSize *int
	CleanDb    *bool
	Filename   *string
	SqlTable   *string
	// url            *string
	// delay          *int32
	// maxMessageSize *int32
	// aws            AwsParams
}

type ListObjectParams struct {
	Aws AwsParams
}

type Counter struct {
	Total      Calls
	Concurrent Calls
	Error      Calls
}

type Channels struct {
	Calls    Counter
	Channels chan *s3.Client
}

type Output struct {
	FileStream io.Writer
}

type S3StreamingLister struct {
	Config          Config
	InputConcurrent int32
	Clients         Channels
	Output          Output
}

var GitCommit string
var Version string

func DefaultS3StreamingLister() *S3StreamingLister {
	prefix := ""
	delimiter := "/"
	mjson := "mjson"
	outputSqsUrl := ""
	outputSqsDelay := int32(10)
	outputSqsMaxMessageSize := 137715
	// bucket := nil
	keyId := ""
	secretAccessKey := ""
	sessionToken := ""
	region := "eu-central-1"
	s3Workers := 16
	s3DeleteWorkers := 16
	s3DeleteChunkSize := 1000
	sqsChunkSize := 1000
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
	commitSize := 2000
	frontend := "aws-s3"
	sqlQuery := "select key, mtime, size from %s"
	app := S3StreamingLister{
		Config: Config{
			Version:   Version,
			GitCommit: GitCommit,
			Prefix:    &prefix,
			Delimiter: &delimiter,
			Strategy:  &strategy,
			Prefixes:  &prefixes,
			Format:    &mjson,
			Frontend: FrontendParams{
				Frontend: &frontend,
				Sqlite: SqliteFrontendParams{
					Filename:  &sqlFilename,
					Query:     &sqlQuery,
					TableName: &sqlTables,
				},
			},
			Output: OutputParams{
				Sqlite: SqliteParams{
					CommitSize: &commitSize,
					Workers:    &sqlWorkers,
					CleanDb:    &falseVal,
					Filename:   &sqlFilename,
					SqlTable:   &sqlTables,
				},
				Sqs: SqsParams{
					ChunkSize:      &sqsChunkSize,
					Workers:        &outputSqsWorkers,
					Delay:          &outputSqsDelay,
					Url:            &outputSqsUrl,
					MaxMessageSize: &outputSqsMaxMessageSize,
					Aws: AwsParams{
						KeyId:           &keyId,
						SecretAccessKey: &secretAccessKey,
						SessionToken:    &sessionToken,
						Region:          &region,
					},
				},
				S3Delete: S3DeleteParams{
					Workers:   &s3DeleteWorkers,
					ChunkSize: &s3DeleteChunkSize,
					Aws: AwsParams{
						KeyId:           &keyId,
						SecretAccessKey: &secretAccessKey,
						SessionToken:    &sessionToken,
						Region:          &region,
					},
				},
				DynamoDb: DynamoDbParams{
					Workers: &s3DeleteWorkers,
					Aws: AwsParams{
						KeyId:           &keyId,
						SecretAccessKey: &secretAccessKey,
						SessionToken:    &sessionToken,
						Region:          &region,
					},
				},
			},
			Lambda: LambdaParams{
				Deploy: &falseVal,
				Start:  &falseVal,
				Aws: AwsParams{
					KeyId:           &keyId,
					SecretAccessKey: &secretAccessKey,
					SessionToken:    &sessionToken,
					Region:          &region,
				},
			},
			Bucket:        nil,
			S3Workers:     &s3Workers,
			MaxKeys:       &maxKeys,
			StatsFragment: &statsFragment,
			Progress:      &progress,
			ListObject: ListObjectParams{
				Aws: AwsParams{
					KeyId:           &keyId,
					SecretAccessKey: &secretAccessKey,
					SessionToken:    &sessionToken,
					Region:          &region,
				},
			},
		},
		InputConcurrent: 0,
		Clients: Channels{
			Calls: Counter{
				Total:      Calls{},
				Concurrent: Calls{},
			},
			Channels: make(chan *s3.Client, s3Workers),
		},
		Output: Output{
			FileStream: os.Stdout,
			// aws:        *aws.NewConfig(),
		},
	}
	return &app
}

func versionStr(args *Config) string {
	return fmt.Sprintf("Version: %s:%s\n", args.Version, args.GitCommit)
}

func ParseArgs(app *S3StreamingLister, osArgs []string) error {
	rootCmd := &cobra.Command{
		Use:     path.Base(osArgs[0]),
		Short:   "s3-streaming-lister short help",
		Long:    strings.TrimSpace("s3-streaming-lister long help"),
		Version: versionStr(&app.Config),
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
	app.Config.Strategy = flags.String("strategy", *app.Config.Strategy, "delimiter | letter")
	app.Config.Prefixes = flags.StringArray("prefixes", *app.Config.Prefixes, "prefixs (safe characters are default, see https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html )")
	app.Config.Prefix = flags.String("prefix", *app.Config.Prefix, "aws prefix")
	app.Config.Delimiter = flags.String("delimiter", *app.Config.Delimiter, "aws delimiter")
	app.Config.Format = flags.String("format", *app.Config.Format, "mjson | sqs | awsls")

	app.Config.Output.Sqs.Url = flags.String("outputSqsUrl", *app.Config.Output.Sqs.Url, "url")
	app.Config.Output.Sqs.Delay = flags.Int32("outputSqsDelay", *app.Config.Output.Sqs.Delay, "delay")
	app.Config.Output.Sqs.MaxMessageSize = flags.Int("outputSqsMaxMessageSize", *app.Config.Output.Sqs.MaxMessageSize, "maxMessageSize")
	app.Config.Output.Sqs.Workers = flags.Int("outputSqsWorkers", *app.Config.Output.Sqs.Workers, "number of output sqs workers")
	app.Config.Output.Sqs.ChunkSize = flags.Int("outputSqsChunkSize", *app.Config.Output.Sqs.ChunkSize, "size of typical object chunks")

	app.Config.Bucket = flags.StringP("bucket", "b", "", "aws bucket name")
	app.Config.MaxKeys = flags.Int("maxKeys", *app.Config.MaxKeys, "aws maxKey pageElement size 1000")
	app.Config.S3Workers = flags.Int("s3Worker", *app.Config.S3Workers, "number of query workers")
	app.Config.Output.S3Delete.Workers = flags.Int("outputS3DeleteWorkers", *app.Config.Output.S3Delete.Workers, "number of output s3 delete workers")
	app.Config.Output.S3Delete.ChunkSize = flags.Int("outputS3DeleteChunkSize", *app.Config.Output.S3Delete.ChunkSize, "size of chunks send to s3 delete api")
	app.Config.StatsFragment = flags.Uint64("statsFragment", *app.Config.StatsFragment, "number statistics output")
	app.Config.Progress = flags.Bool("progress", *app.Config.Progress, "progress output")
	rootCmd.MarkFlagRequired("bucket")

	app.Config.Output.Sqlite.CleanDb = flags.Bool("sqliteCleanDb", *app.Config.Output.Sqlite.CleanDb, "set cleandb")
	app.Config.Output.Sqlite.Workers = flags.Int("sqliteWorkers", *app.Config.Output.Sqlite.Workers, "writer workers")
	app.Config.Output.Sqlite.Filename = flags.String("sqliteFilename", *app.Config.Output.Sqlite.Filename, "sqlite filename")
	app.Config.Output.Sqlite.SqlTable = flags.String("sqliteTable", *app.Config.Output.Sqlite.SqlTable, "sqlite table name")
	app.Config.Output.Sqlite.CommitSize = flags.Int("sqliteCommitSize", *app.Config.Output.Sqlite.CommitSize, "sqlite table name")

	app.Config.Lambda.Start = flags.Bool("lambdaStart", *app.Config.Lambda.Start, "start lambda")
	app.Config.Lambda.Deploy = flags.Bool("lambdaDeploy", *app.Config.Lambda.Deploy, "deploy the lambda")

	app.Config.Frontend.Frontend = flags.String("frontend", *app.Config.Frontend.Frontend, "aws-s3 sqlite")

	app.Config.Frontend.Sqlite.Filename = flags.String("frontendSqliteFilename", *app.Config.Frontend.Sqlite.Filename, "file.sql")
	app.Config.Frontend.Sqlite.Query = flags.String("frontendSqliteQuery", *app.Config.Frontend.Sqlite.Query, "sql query")

	flagsAws("lambda", flags, &app.Config.Lambda.Aws)
	flagsAws("listObject", flags, &app.Config.ListObject.Aws)
	flagsAws("outputSqs", flags, &app.Config.Output.Sqs.Aws)
	flagsAws("outputS3Delete", flags, &app.Config.Output.S3Delete.Aws)

	rootCmd.SetArgs(osArgs[1:])
	err := rootCmd.Execute()

	app.Config.Help = rootCmd.Flags().Lookup("help").Value.String() == "true"
	app.Config.VersionFlag = rootCmd.Flags().Lookup("version").Value.String() == "true"

	return err
}

func flagsAws(prefix string, flags *pflag.FlagSet, awsParams *AwsParams) {
	defaultAWSAccessKeyId := *awsParams.KeyId
	defaultAWSSecretAccessKey := *awsParams.SecretAccessKey
	defaultAWSSessionToken := *awsParams.SessionToken
	defaultAWSRegion := *awsParams.Region
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
	awsParams.KeyId = flags.String(fmt.Sprintf("%sAwsAccessKeyId", prefix), defaultAWSAccessKeyId, strings.Join(envKeyIds, ","))
	awsParams.SecretAccessKey = flags.String(fmt.Sprintf("%sAwsSecretAccessKey", prefix), defaultAWSSecretAccessKey, strings.Join(envSecretAccessKeys, ","))
	awsParams.SessionToken = flags.String(fmt.Sprintf("%sAwsSessionToken", prefix), defaultAWSSessionToken, strings.Join(envSessionTokens, ","))
	awsParams.Region = flags.String(fmt.Sprintf("%sAwsRegion", prefix), defaultAWSRegion, strings.Join(envRegions, ","))
}

type MyCredentials struct {
	cred aws.Credentials
}

func (my *MyCredentials) Retrieve(ctx context.Context) (aws.Credentials, error) {
	return my.cred, nil
}

func InitS3StreamingLister(app *S3StreamingLister) {
	err := ParseArgs(app, os.Args)
	if err != nil {
		panic("cobra error, " + err.Error())
	}
	if app.Config.Help || app.Config.VersionFlag {
		os.Exit(0)
		return
	}
	// fmt.Fprintf(os.Stderr, "XXX:%p:%s", app.Config.bucket, *app.Config.bucket)
	if app.Config.Bucket == nil || len(*app.Config.Bucket) == 0 || app.Config.Help {
		os.Exit(1)
		return
	}
	for _, awsParams := range []*AwsParams{
		&app.Config.Lambda.Aws,
		&app.Config.ListObject.Aws,
		&app.Config.Output.Sqs.Aws,
		&app.Config.Output.S3Delete.Aws} {
		// awsConfig, err := config.LoadOptions(context.TODO()) // config.WithRegion(*app.Config.region),

		// if err != nil {
		// 	panic("aws configuration error, " + err.Error())
		// }

		awsCredProvider := MyCredentials{
			cred: aws.Credentials{
				AccessKeyID:     *awsParams.KeyId,
				SecretAccessKey: *awsParams.SecretAccessKey,
				SessionToken:    *awsParams.SessionToken,
			},
		}
		// fmt.Fprintln(os.Stderr, "Region=", *awsParams.region)
		awsParams.Cfg = aws.Config{
			Credentials: &awsCredProvider,
			// func (fn CredentialsProviderFunc) Retrieve(ctx context.Context) (Credentials, error) {
			// return fn(ctx)
			// },
			Region: *awsParams.Region,
		}
	}
}
