package main

import (
	"fmt"

	"github.com/pulumi/pulumi-aws/sdk/v5/go/aws"
	"github.com/pulumi/pulumi-aws/sdk/v5/go/aws/cloudwatch"
	"github.com/pulumi/pulumi-aws/sdk/v5/go/aws/iam"
	"github.com/pulumi/pulumi-aws/sdk/v5/go/aws/lambda"
	"github.com/pulumi/pulumi-aws/sdk/v5/go/aws/sqs"

	"github.com/pulumi/pulumi-command/sdk/go/command/local"

	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
)

func buildZip(ctx *pulumi.Context, path string) (*local.Command, error) {
	gobuild, err := local.NewCommand(ctx, "build-lambda", &local.CommandArgs{
		Create: pulumi.String(fmt.Sprintf(`
			cd %s && \
			env && \
			go build ./main.go && \
			zip -o lambda.zip main
		`, path)),
		Dir: nil,
		Environment: pulumi.StringMap{
			"GOOS":        pulumi.String("linux"),
			"GOARCH":      pulumi.String("amd64"),
			"CGO_ENABLED": pulumi.String("0"),
		},
		// Delete:      nil,
		// Interpreter: nil,
		// Stdin:       nil,
		// Triggers:    nil,
	})
	if err != nil {
		return nil, err
	}
	ctx.Export("go build stdout", gobuild.Stdout.ToStringOutput())
	ctx.Export("go build stderr", gobuild.Stderr.ToStringOutput())
	// res2, _ := NewMyResource(ctx, "res2", &MyResourceArgs{/*...*/}, pulumi.DependsOn([]Resource{res1}))

	// archive, err := os.Create("lambda.zip")
	// if err != nil {
	// 	return err
	// }
	// defer archive.Close()
	// zipWriter := zip.NewWriter(archive)

	// f1, err := os.Open("../handler/main")
	// if err != nil {
	// 	return err
	// }
	// defer f1.Close()

	// w1, err := zipWriter.Create("main")
	// if err != nil {
	// 	return err
	// }
	// if _, err := io.Copy(w1, f1); err != nil {
	// 	return err
	// }
	// zipWriter.Close()
	return gobuild, nil
}

func main() {

	pulumi.Run(func(ctx *pulumi.Context) error {
		current, err := aws.GetCallerIdentity(ctx, nil, nil)
		if err != nil {
			return err
		}
		// region, err := aws.GetRegion(ctx, nil, nil)
		// if err != nil {
		// 	return err
		// }

		queueName := "Eimer-Kette"
		queue, err := sqs.NewQueue(ctx, "eimer-kette", &sqs.QueueArgs{
			Name: pulumi.StringPtr(queueName),
		})
		if err != nil {
			return err
		}

		qaccessName := fmt.Sprintf("%s-qaccess", queueName)

		qaccess, err := iam.NewRole(ctx, qaccessName, &iam.RoleArgs{
			Name: pulumi.StringPtr(qaccessName),
			AssumeRolePolicy: pulumi.String(`{
						"Version": "2012-10-17",
						"Statement": [
							{
								"Effect": "Allow",
								"Principal": {
									"Service": "lambda.amazonaws.com"
								},
								"Action": "sts:AssumeRole"
							}
						]
					}`),
		})
		if err != nil {
			return err
		}
		iam.NewRolePolicyAttachment(ctx, "ecr-power-user", &iam.RolePolicyAttachmentArgs{
			Role:      qaccess,
			PolicyArn: iam.ManagedPolicyAWSLambdaBasicExecutionRole,
		})

		policy := pulumi.Sprintf(`{
			"Version": "2012-10-17",
			"Statement": {
				"Action": [
					"sqs:DeleteMessage",
					"sqs:ReceiveMessage",
					"sqs:SendMessage",
					"sqs:GetQueueAttributes"
				],
				"Resource": "arn:aws:sqs:*:%s:%s",
				"Effect":   "Allow"
			}
		}`, current.AccountId, queueName)
		policyName := fmt.Sprintf("%s", qaccessName)
		_, err = iam.NewRolePolicy(ctx, policyName, &iam.RolePolicyArgs{
			Name:   pulumi.StringPtr(policyName),
			Role:   qaccess,
			Policy: policy,
		})
		if err != nil {
			return err
		}
		version := "DEV"

		lambdaName := fmt.Sprintf("%s-lambda", queueName)
		lambaLogs, err := cloudwatch.NewLogGroup(ctx, lambdaName, &cloudwatch.LogGroupArgs{
			RetentionInDays: pulumi.Int(14),
		})

		lambdaFn, err := lambda.NewFunction(ctx, lambdaName, &lambda.FunctionArgs{
			Code:    pulumi.NewFileArchive("../handler/lambda.zip"),
			Role:    qaccess.Arn,
			Handler: pulumi.String("main"),
			Runtime: pulumi.String("go1.x"),
			Environment: &lambda.FunctionEnvironmentArgs{
				Variables: pulumi.StringMap{
					"VERSION":       pulumi.String(version),
					"AWS_SQS_QUEUE": queue.Url,
					// "AWS_REGION":    pulumi.String(region.Name),
				},
			},
		}, pulumi.DependsOn([]pulumi.Resource{
			lambaLogs,
		}))
		// }, pulumi.DependsOn([]pulumi.Resource{gobuild}))
		if err != nil {
			return err
		}

		_, err = lambda.NewEventSourceMapping(ctx, lambdaName, &lambda.EventSourceMappingArgs{
			EventSourceArn: queue.Arn,
			FunctionName:   lambdaFn.Arn,
		})
		if err != nil {
			return err
		}
		ctx.Export("queue", queue.Url)
		return nil
	})
}
