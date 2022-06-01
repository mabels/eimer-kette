package eimerkette

import (
	"context"
	"log"
	"os"
	"sync"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	mylambda "github.com/mabels/eimer-kette/lambda"
)

func isLambda() bool {
	for _, envKey := range []string{
		"AWS_EXECUTION_ENV",
		"AWS_LAMBDA_FUNCTION_NAME",
		"AWS_LAMBDA_LOG_STREAM_NAME",
		"AWS_LAMBDA_FUNCTION_MEMORY_SIZE",
		"AWS_LAMBDA_FUNCTION_VERSION",
		"LAMBDA_TASK_ROOT",
		"LAMBDA_RUNTIME_DIR",
		"AWS_LAMBDA_LOG_GROUP_NAME",
		"AWS_LAMBDA_RUNTIME_API",
		"AWS_LAMBDA_INITIALIZATION_TYPE",
		"_LAMBDA_SERVER_PORT",
	} {
		if _, ok := os.LookupEnv(envKey); !ok {
			return false
		}
	}
	return true
}

func LambdaMain() {
	if !isLambda() {
		return
	}
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatalf("configuration error; %v", err)
	}
	handler := mylambda.HandlerCtx{
		QueueUrl:      os.Getenv("AWS_SQS_QUEUE"),
		SqsClient:     sqs.NewFromConfig(cfg),
		S3ClientsSync: sync.Mutex{},
	}
	lambda.Start(mylambda.HandlerWithContext(&handler))

	os.Exit(0)
}
