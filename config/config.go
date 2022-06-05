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
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type BucketParams struct {
	Name *string
	Aws  AwsParams
}

type Config struct {
	GitCommit     string
	Version       string
	Strategy      *string
	Prefixes      *[]string
	Prefix        *string
	Delimiter     *string
	Format        *string
	Bucket        BucketParams
	MaxKeys       *int
	S3Workers     *int
	StatsFragment *uint64
	Help          bool
	VersionFlag   bool
	Progress      *int
	Output        OutputParams
	ListObject    ListObjectParams
	// Lambda        LambdaParams
	Frontend FrontendParams
}

type FrontendParams struct {
	Sqlite               SqliteFrontendParams
	Parquet              ParquetFrontendParams
	Frontend             *string
	AwsLambdaLister      AwsLambdaListerParams
	AwsLambdaCreateFiles AwsLambdaCreateFileParams
}

type AwsLambdaListerParams struct {
	BackChannelQ SqsParams
	CommandQ     SqsParams
}

type AwsLambdaCreateFileParams struct {
	NumberOfFiles *int64
	JobConcurrent *int
	JobSize       *int
	BackChannelQ  SqsParams
	CommandQ      SqsParams
}

type ParquetFrontendParams struct {
	Filename  *string
	Workers   *int
	RowBuffer *int
}

type SqliteFrontendParams struct {
	Filename  *string
	Query     *string
	TableName *string
}

type ParquetParams struct {
	Workers   *int
	ChunkSize *int
	FileName  *string
}

type OutputParams struct {
	Sqs      SqsParams
	Sqlite   SqliteParams
	Parquet  ParquetParams
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
	// FileName *string
}

type EimerKette struct {
	Config          Config
	InputConcurrent int32
	Clients         Channels
	Output          Output
}

var GitCommit string
var Version string

func DefaultS3StreamingLister() *EimerKette {
	prefix := ""
	delimiter := "/"
	mjson := "mjson"
	outputSqsUrl := ""
	outputSqsDelay := int32(10)
	outputSqsMaxMessageSize := 137715
	s3DeleteWorkers := 16
	s3DeleteChunkSize := 1000
	sqsChunkSize := 1000
	sqlWorkers := 8
	sqliteCommitSize := 1000
	outputSqsWorkers := 2

	// bucket := ""
	keyId := ""
	secretAccessKey := ""
	sessionToken := ""
	region := "eu-central-1"
	s3Workers := 16
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
		"/", "!", "-", "_", ".", "*", "'", "(", ")", "=",
	}
	strategy := "delimiter"
	progress := 5
	sqlFilename := "./file.sql"
	parquetWorkers := 4
	parquetRowBuffer := 10000
	parquetFilename := "./file.parquet"
	// parquetFilename := "./files.parquet"
	// commitSize := 2000
	frontend := "aws-s3"
	sqlQuery := "select key, mtime, size from %s"

	numberOfFiles := int64(0)
	jobConcurrent := int(1)
	jobSize := int(100)
	app := EimerKette{
		Config: Config{
			GitCommit:     GitCommit,
			Version:       Version,
			Strategy:      &strategy,
			Prefixes:      &prefixes,
			Prefix:        &prefix,
			Delimiter:     &delimiter,
			Format:        &mjson,
			Bucket:        BucketParams{Name: nil, Aws: AwsParams{KeyId: new(string), SecretAccessKey: new(string), SessionToken: new(string), Region: &region, Cfg: aws.Config{}}},
			MaxKeys:       &maxKeys,
			S3Workers:     &s3Workers,
			StatsFragment: &statsFragment,
			Help:          false,
			VersionFlag:   false,
			Progress:      &progress,
			Output: OutputParams{
				Sqs: SqsParams{
					Url:            &outputSqsUrl,
					Delay:          &outputSqsDelay,
					MaxMessageSize: &outputSqsMaxMessageSize,
					ChunkSize:      &sqsChunkSize,
					Workers:        &outputSqsWorkers,
					Aws: AwsParams{
						KeyId:           new(string),
						SecretAccessKey: new(string),
						SessionToken:    new(string),
						Region:          new(string),
						Cfg:             aws.Config{},
					},
				},
				Sqlite: SqliteParams{
					Workers:    &sqlWorkers,
					CommitSize: &sqliteCommitSize,
					CleanDb:    &falseVal,
					Filename:   new(string),
					SqlTable:   new(string),
				},
				Parquet: ParquetParams{
					Workers:   &parquetWorkers,
					ChunkSize: &parquetRowBuffer,
				},
				S3Delete: S3DeleteParams{
					Workers:   &s3DeleteWorkers,
					ChunkSize: &s3DeleteChunkSize,
					Aws: AwsParams{
						KeyId:           new(string),
						SecretAccessKey: new(string),
						SessionToken:    new(string),
						Region:          new(string),
						Cfg:             aws.Config{},
					},
				},
				DynamoDb: DynamoDbParams{},
			},
			ListObject: ListObjectParams{Aws: AwsParams{KeyId: &keyId, SecretAccessKey: &secretAccessKey, SessionToken: &sessionToken, Region: &region}},
			// Lambda:     LambdaParams{Deploy: &falseVal, Start: &falseVal, Aws: AwsParams{KeyId: &keyId, SecretAccessKey: &secretAccessKey, SessionToken: &sessionToken, Region: &region}},
			Frontend: FrontendParams{
				Frontend: &frontend,
				Parquet: ParquetFrontendParams{
					Filename:  &parquetFilename,
					Workers:   &parquetWorkers,
					RowBuffer: &parquetRowBuffer,
				},
				Sqlite: SqliteFrontendParams{
					Filename:  &sqlFilename,
					Query:     &sqlQuery,
					TableName: &sqlTables,
				},
				AwsLambdaLister: AwsLambdaListerParams{
					BackChannelQ: SqsParams{Workers: new(int),
						ChunkSize:      new(int),
						Url:            new(string),
						Delay:          new(int32),
						MaxMessageSize: new(int),
						Aws: AwsParams{KeyId: new(string),
							SecretAccessKey: new(string),
							SessionToken:    new(string),
							Region:          new(string),
						}},
					CommandQ: SqsParams{
						Workers:        new(int),
						ChunkSize:      new(int),
						Url:            new(string),
						Delay:          new(int32),
						MaxMessageSize: new(int),
						Aws: AwsParams{KeyId: new(string),
							SecretAccessKey: new(string),
							SessionToken:    new(string),
							Region:          new(string),
						}}},
				AwsLambdaCreateFiles: AwsLambdaCreateFileParams{
					NumberOfFiles: &numberOfFiles,
					JobConcurrent: &jobConcurrent,
					JobSize:       &jobSize,
					BackChannelQ: SqsParams{
						Workers:        new(int),
						ChunkSize:      new(int),
						Url:            new(string),
						Delay:          new(int32),
						MaxMessageSize: new(int),
						Aws: AwsParams{
							KeyId:           new(string),
							SecretAccessKey: new(string),
							SessionToken:    new(string),
							Region:          new(string),
						}},
					CommandQ: SqsParams{
						Workers:        new(int),
						ChunkSize:      new(int),
						Url:            new(string),
						Delay:          new(int32),
						MaxMessageSize: new(int),
						Aws: AwsParams{
							KeyId:           new(string),
							SecretAccessKey: new(string),
							SessionToken:    new(string),
							Region:          new(string),
						}},
				}},
		},
		InputConcurrent: 0,
		Clients: Channels{
			Calls: Counter{
				Total:      *MakeCalls(),
				Concurrent: *MakeCalls(),
				Error:      *MakeCalls(),
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

func flagsAwsSqs(prefix string, flags *pflag.FlagSet, sqs *SqsParams) {
	sqs.Url = flags.String(fmt.Sprintf("%sSqsUrl", prefix), *sqs.Url, fmt.Sprintf("%s url", prefix))
	sqs.Delay = flags.Int32(fmt.Sprintf("%sSqsDelay", prefix), *sqs.Delay, fmt.Sprintf("%s delay", prefix))
	sqs.MaxMessageSize = flags.Int(fmt.Sprintf("%sSqsMaxMessageSize", prefix), *sqs.MaxMessageSize, fmt.Sprintf("%s maxMessageSize", prefix))
	sqs.Workers = flags.Int(fmt.Sprintf("%sSqsWorkers", prefix), *sqs.Workers, fmt.Sprintf("%s number of output sqs workers", prefix))
	sqs.ChunkSize = flags.Int(fmt.Sprintf("%sSqsChunkSize", prefix), *sqs.ChunkSize, fmt.Sprintf("%s size of typical object chunks", prefix))
}

func ParseArgs(app *EimerKette, osArgs []string) error {
	rootCmd := &cobra.Command{
		Use:     path.Base(osArgs[0]),
		Short:   "eimer-kette short help",
		Long:    strings.TrimSpace("eimer-kette long help"),
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
	app.Config.Format = flags.String("format", *app.Config.Format, "mjson | sqs | awsls | sqlite | dynamo | s3delete | parquet")

	flagsAwsSqs("output", flags, &app.Config.Output.Sqs)

	app.Config.Bucket.Name = flags.StringP("bucket", "b", "", "aws bucket name")
	flagsAws("Bucket", flags, &app.Config.Bucket.Aws)
	app.Config.MaxKeys = flags.Int("maxKeys", *app.Config.MaxKeys, "aws maxKey pageElement size 1000")
	app.Config.S3Workers = flags.Int("s3Worker", *app.Config.S3Workers, "number of query workers")
	app.Config.Output.S3Delete.Workers = flags.Int("outputS3DeleteWorkers", *app.Config.Output.S3Delete.Workers, "number of output s3 delete workers")
	app.Config.Output.S3Delete.ChunkSize = flags.Int("outputS3DeleteChunkSize", *app.Config.Output.S3Delete.ChunkSize, "size of chunks send to s3 delete api")
	app.Config.StatsFragment = flags.Uint64("statsFragment", *app.Config.StatsFragment, "number statistics output")
	app.Config.Progress = flags.Int("progress", *app.Config.Progress, "progress output every x seconds")
	rootCmd.MarkFlagRequired("bucket")

	app.Config.Output.Parquet.Workers = flags.Int("parquetWorkers", *app.Config.Output.Parquet.Workers, "writer workers")
	app.Config.Output.Parquet.ChunkSize = flags.Int("parquetChunkSize", *app.Config.Output.Parquet.ChunkSize, "writer workers")
	app.Config.Output.Parquet.FileName = flags.String("parquetFilename", "", "parque output filename default stdout")

	app.Config.Output.Sqlite.CleanDb = flags.Bool("sqliteCleanDb", *app.Config.Output.Sqlite.CleanDb, "set cleandb")
	app.Config.Output.Sqlite.Workers = flags.Int("sqliteWorkers", *app.Config.Output.Sqlite.Workers, "writer workers")
	app.Config.Output.Sqlite.Filename = flags.String("sqliteFilename", *app.Config.Output.Sqlite.Filename, "sqlite filename")
	app.Config.Output.Sqlite.SqlTable = flags.String("sqliteTable", *app.Config.Output.Sqlite.SqlTable, "sqlite table name")
	app.Config.Output.Sqlite.CommitSize = flags.Int("sqliteCommitSize", *app.Config.Output.Sqlite.CommitSize, "sqlite table name")

	// app.Config.Lambda.Start = flags.Bool("lambdaStart", *app.Config.Lambda.Start, "start lambda")
	// app.Config.Lambda.Deploy = flags.Bool("lambdaDeploy", *app.Config.Lambda.Deploy, "deploy the lambda")

	app.Config.Frontend.Frontend = flags.String("frontend", *app.Config.Frontend.Frontend, "aws-s3 | sqlite | parquet")

	app.Config.Frontend.Sqlite.Filename = flags.String("frontendSqliteFilename", *app.Config.Frontend.Sqlite.Filename, "file.sql")
	app.Config.Frontend.Sqlite.Query = flags.String("frontendSqliteQuery", *app.Config.Frontend.Sqlite.Query, "sql query")

	app.Config.Frontend.Parquet.Filename = flags.String("frontendParquetFilename", *app.Config.Frontend.Parquet.Filename, "file.sql")
	app.Config.Frontend.Parquet.Workers = flags.Int("frontendParquetWorkers", *app.Config.Frontend.Parquet.Workers, "parquet worker")
	app.Config.Frontend.Parquet.RowBuffer = flags.Int("frontendParquetRowBuffer", *app.Config.Frontend.Parquet.RowBuffer, "parquet row buffer")

	// flagsAws("lambda", flags, &app.Config.Lambda.Aws)

	flagsAws("awsLambdaListerCommand", flags, &app.Config.Frontend.AwsLambdaLister.CommandQ.Aws)
	flagsAwsSqs("awsLambdaListerCommand", flags, &app.Config.Frontend.AwsLambdaLister.CommandQ)
	flagsAws("awsLambdaListerBackchannel", flags, &app.Config.Frontend.AwsLambdaLister.BackChannelQ.Aws)
	flagsAwsSqs("awsLambdaListerBackchannel", flags, &app.Config.Frontend.AwsLambdaLister.BackChannelQ)

	app.Config.Frontend.AwsLambdaCreateFiles.JobConcurrent = flags.Int("AwsLambdaCreateFilesJobConcurrent", *app.Config.Frontend.AwsLambdaCreateFiles.JobConcurrent, "AwsLambdaCreateFiles JobConcurrent")
	app.Config.Frontend.AwsLambdaCreateFiles.JobSize = flags.Int("AwsLambdaCreateFilesJobSize", *app.Config.Frontend.AwsLambdaCreateFiles.JobSize, "AwsLambdaCreateFiles JobSize")
	app.Config.Frontend.AwsLambdaCreateFiles.NumberOfFiles = flags.Int64("AwsLambdaCreateFilesNumberOfFiles", *app.Config.Frontend.AwsLambdaCreateFiles.NumberOfFiles, "AwsLambdaCreateFiles NumberOfFiles")

	flagsAws("awsLambdaCreateFilesCommand", flags, &app.Config.Frontend.AwsLambdaCreateFiles.CommandQ.Aws)
	flagsAwsSqs("awsLambdaCreateFilesCommand", flags, &app.Config.Frontend.AwsLambdaCreateFiles.CommandQ)
	flagsAws("awsLambdaCreateFilesBackchannel", flags, &app.Config.Frontend.AwsLambdaCreateFiles.BackChannelQ.Aws)
	flagsAwsSqs("awsLambdaCreateFilesBackchannel", flags, &app.Config.Frontend.AwsLambdaCreateFiles.BackChannelQ)

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

func InitS3StreamingLister(app *EimerKette) {
	err := ParseArgs(app, os.Args)
	if err != nil {
		panic("cobra error, " + err.Error())
	}
	if app.Config.Help || app.Config.VersionFlag {
		os.Exit(0)
		return
	}
	// fmt.Fprintf(os.Stderr, "XXX:%p:%s", app.Config.bucket, *app.Config.bucket)
	if app.Config.Bucket.Name == nil || len(*app.Config.Bucket.Name) == 0 || app.Config.Help {
		os.Exit(1)
		return
	}
	for _, awsParams := range []*AwsParams{
		&app.Config.Frontend.AwsLambdaCreateFiles.BackChannelQ.Aws,
		&app.Config.Frontend.AwsLambdaCreateFiles.CommandQ.Aws,
		&app.Config.Frontend.AwsLambdaLister.BackChannelQ.Aws,
		&app.Config.Frontend.AwsLambdaLister.CommandQ.Aws,
		&app.Config.Bucket.Aws,
		&app.Config.ListObject.Aws,
		&app.Config.Output.Sqs.Aws,
		&app.Config.Output.S3Delete.Aws} {
		// awsConfig, err := config.LoadOptions(context.TODO()) // config.WithRegion(*app.Config.region),

		// if err != nil {
		// 	panic("aws configuration error, " + err.Error())
		// }

		if awsParams.KeyId == nil || len(*awsParams.KeyId) == 0 ||
			awsParams.SecretAccessKey == nil || len(*awsParams.SecretAccessKey) == 0 ||
			awsParams.SessionToken == nil || len(*awsParams.SessionToken) == 0 {
			cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(*awsParams.Region))
			if err != nil {
				println("could not load aws config")
			}
			awsParams.Cfg = cfg
		} else {
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
}
