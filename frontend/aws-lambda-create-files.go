package frontend

import (
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	config "github.com/mabels/eimer-kette/config"
	"github.com/mabels/eimer-kette/lambda"
	"github.com/mabels/eimer-kette/models"
	myq "github.com/mabels/eimer-kette/my-queue"
	"github.com/mabels/eimer-kette/status"
)

func AwsLambdaCreateFiles(app *config.EimerKette, cho myq.MyQueue, chstatus myq.MyQueue) {
	// Setup BackChannel
	lambda.StartBackChannel(app, &app.Config.Frontend.AwsLambdaCreateFiles.BackChannelQ, cho, chstatus)
	// Issue Command
	// app.Config.Frontend.AwsLambdaLister.CommandQ =
	// out, err := json.MarshalIndent(app.Config.Frontend.AwsLambdaCreateFiles, "", "  ")
	// fmt.Fprintf(os.Stderr, "AwsLambdaCreateFiles:%s\n%v\n%v\n", string(out), app.Config.Frontend.AwsLambdaCreateFiles, err)
	bc := lambda.BackChannel{
		Sqs: sqs.NewFromConfig(app.Config.Frontend.AwsLambdaCreateFiles.CommandQ.Aws.Cfg),
		Url: *app.Config.Frontend.AwsLambdaCreateFiles.CommandQ.Url,
	}

	msg := models.CreateFilesPayload{
		Bucket: models.BucketParams{
			Name: *app.Config.Bucket.Name,
			Credentials: &models.AwsCredentials{
				KeyId:        app.Config.Bucket.Aws.KeyId,
				AccessKey:    app.Config.Bucket.Aws.SecretAccessKey,
				Region:       app.Config.Bucket.Aws.Region,
				SessionToken: new(string),
			},
		},
		NumberOfFiles: *app.Config.Frontend.AwsLambdaCreateFiles.NumberOfFiles,
		JobSize:       *app.Config.Frontend.AwsLambdaCreateFiles.JobSize,
		JobConcurrent: *app.Config.Frontend.AwsLambdaCreateFiles.JobConcurrent,
		SkipCreate:    false,
		ScheduleTime:  0,
		BackChannel: models.SqsParams{
			Url: *app.Config.Frontend.AwsLambdaCreateFiles.BackChannelQ.Url,
			Credentials: &models.AwsCredentials{
				KeyId:        app.Config.Frontend.AwsLambdaCreateFiles.BackChannelQ.Aws.KeyId,
				AccessKey:    app.Config.Frontend.AwsLambdaCreateFiles.BackChannelQ.Aws.SecretAccessKey,
				Region:       app.Config.Bucket.Aws.Region,
				SessionToken: new(string),
			},
		},
	}

	infoStr := "XXXX"
	bc.SendResult(status.RunResult{
		Action:  "Info:writeSingleFiles",
		Info:    &infoStr,
		Related: msg,
	})

	_, err := bc.SendCmdCreateFiles(&msg)
	if err != nil {
		chstatus.Push(status.RunStatus{Err: err})
		return
	}

	/*
		sqsClient
		jsonMsg, err := json.MarshalIndent(msg, "", "  ")
		if err != nil {
			chstatus.Push(status.RunStatus{Err: err})
			return
		}
		jsonStr := string(jsonMsg)
		fmt.Fprintf(os.Stderr, "SendMessage:%s\n", jsonStr)
		_, err = sqsClient.SendMessage(context.TODO(), &sqs.SendMessageInput{
			MessageBody: &jsonStr,
			// MessageGroupId: aws.String("XyZ"), // lambda.GetGroupId(msg),
			QueueUrl: app.Config.Frontend.AwsLambdaCreateFiles.CommandQ.Url,
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "SendCmd:%s:%v\n", *app.Config.Frontend.AwsLambdaCreateFiles.CommandQ.Url, err)
			chstatus.Push(status.RunStatus{Err: err})
			return
		}
	*/
}
