package eimerkette

import (
	"fmt"
	"os"

	"github.com/pulumi/pulumi-aws/sdk/v5/go/aws"
	"github.com/pulumi/pulumi-aws/sdk/v5/go/aws/cloudwatch"
	"github.com/pulumi/pulumi-aws/sdk/v5/go/aws/iam"
	"github.com/pulumi/pulumi-aws/sdk/v5/go/aws/lambda"
	"github.com/pulumi/pulumi-aws/sdk/v5/go/aws/s3"
	"github.com/pulumi/pulumi-aws/sdk/v5/go/aws/sqs"

	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
)

// func buildZip(ctx *pulumi.Context, path string) (*local.Command, error) {
// 	gobuild, err := local.NewCommand(ctx, "build-lambda", &local.CommandArgs{
// 		Create: pulumi.String(fmt.Sprintf(`
// 			cd %s && \
// 			env && \
// 			go build ./main.go && \
// 			zip -o lambda.zip main
// 		`, path)),
// 		Dir: nil,
// 		Environment: pulumi.StringMap{
// 			"GOOS":        pulumi.String("linux"),
// 			"GOARCH":      pulumi.String("amd64"),
// 			"CGO_ENABLED": pulumi.String("0"),
// 		},
// 		// Delete:      nil,
// 		// Interpreter: nil,
// 		// Stdin:       nil,
// 		// Triggers:    nil,
// 	})
// 	if err != nil {
// 		return nil, err
// 	}
// 	ctx.Export("go build stdout", gobuild.Stdout.ToStringOutput())
// 	ctx.Export("go build stderr", gobuild.Stderr.ToStringOutput())
// 	// res2, _ := NewMyResource(ctx, "res2", &MyResourceArgs{/*...*/}, pulumi.DependsOn([]Resource{res1}))

// 	// archive, err := os.Create("lambda.zip")
// 	// if err != nil {
// 	// 	return err
// 	// }
// 	// defer archive.Close()
// 	// zipWriter := zip.NewWriter(archive)

// 	// f1, err := os.Open("../handler/main")
// 	// if err != nil {
// 	// 	return err
// 	// }
// 	// defer f1.Close()

// 	// w1, err := zipWriter.Create("main")
// 	// if err != nil {
// 	// 	return err
// 	// }
// 	// if _, err := io.Copy(w1, f1); err != nil {
// 	// 	return err
// 	// }
// 	// zipWriter.Close()
// 	return gobuild, nil
// }

func isPulumi() bool {
	for _, envKey := range []string{
		"PULUMI_PROJECT",
		"PULUMI_STACK",
		"PULUMI_CONFIG_SECRET_KEYS",
		"PULUMI_DRY_RUN",
		"PULUMI_PARALLEL",
		"PULUMI_MONITOR",
		"PULUMI_ENGINE",
	} {
		if _, ok := os.LookupEnv(envKey); !ok {
			return false
		}

	}
	return true
}

func PulumiMain() {
	if !isPulumi() {
		return
	}

	pulumi.Run(func(ctx *pulumi.Context) error {
		current, err := aws.GetCallerIdentity(ctx, nil, nil)
		if err != nil {
			return err
		}
		// region, err := aws.GetRegion(ctx, nil, nil)
		// if err != nil {
		// 	return err
		// }
		// test_volume := "test-volume"
		// testVolumeRole, err := iam.NewRole(ctx, "test-volume-role", &iam.RoleArgs{
		// 	Name: pulumi.StringPtr(fmt.Sprintf("%s-role", test_volume)),
		// 	// AssumeRolePolicy: pulumi.String(`{
		// 	// 			"Version": "2012-10-17",
		// 	// 			"Statement": [
		// 	// 				{
		// 	// 					"Effect": "Allow",
		// 	// 					"Principal": {
		// 	// 						"Service": "lambda.amazonaws.com"
		// 	// 					},
		// 	// 					"Action": "sts:AssumeRole"
		// 	// 				}
		// 	// 			]
		// 	// 		}`),
		// })
		// if err != nil {
		// 	return err
		// }
		// ctx.Export("Stack", ctx.Stack())
		testBucket, err := s3.GetBucket(ctx, "test-volume", pulumi.ID(fmt.Sprintf("eimer-kette-test-%s", current.AccountId)), nil)
		if err != nil {
			testBucket, err = s3.NewBucket(ctx, "test-volume", &s3.BucketArgs{
				Bucket: pulumi.Sprintf("eimer-kette-test-%s", current.AccountId),
			})
			if err != nil {
				return err
			}
		}

		testVolumeUser, err := iam.NewUser(ctx, "test-volume-user", &iam.UserArgs{
			// Name: pulumi.String("test-volume-user"),
		})
		if err != nil {
			return err
		}
		// testVolumeCredentials, err := iam.NewAccessKey(ctx, "test-volume-credentials", &iam.AccessKeyArgs{
		// 	User:   testVolumeUser.Name,
		// 	PgpKey: pulumi.String(strings.Join(strings.Split(`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`, "\n"), ""))})
		// if err != nil {
		// 	return err
		// }

		policy := pulumi.Sprintf(`{
			"Version": "2012-10-17",
			"Statement": {
				"Action": [
					"s3:GetObject",
					"s3:PutObject",
					"s3:DeleteObject"
				],
				"Resource": "arn:aws:s3:::%s/*",
				"Effect":   "Allow"
			}
		}`, testBucket.Bucket)

		_, err = iam.NewUserPolicy(ctx, "test-volume-policy-s3", &iam.UserPolicyArgs{
			Policy: policy,
			User:   testVolumeUser.Name,
		})
		if err != nil {
			return err
		}

		queueName := "Eimer-Kette"
		queue, err := sqs.NewQueue(ctx, "eimer-kette", &sqs.QueueArgs{
			Name: pulumi.StringPtr(queueName),
		})
		if err != nil {
			return err
		}

		qaccessName := fmt.Sprintf("%s-qaccess", queueName)

		qaccess, err := iam.NewRole(ctx, qaccessName, &iam.RoleArgs{
			// Name: pulumi.StringPtr(qaccessName),
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
		iam.NewRolePolicyAttachment(ctx, fmt.Sprintf("%s-lamda", qaccessName), &iam.RolePolicyAttachmentArgs{
			Role:      qaccess,
			PolicyArn: iam.ManagedPolicyAWSLambdaBasicExecutionRole,
		})

		policy = pulumi.Sprintf(`{
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
		policyName := fmt.Sprintf("%s-sqs", qaccessName)
		_, err = iam.NewRolePolicy(ctx, policyName, &iam.RolePolicyArgs{
			Name:   pulumi.StringPtr(policyName),
			Role:   qaccess,
			Policy: policy,
		})
		if err != nil {
			return err
		}
		// version := "DEV"

		lambdaName := fmt.Sprintf("%s-lambda", queueName)
		lambaLogs, err := cloudwatch.NewLogGroup(ctx, lambdaName, &cloudwatch.LogGroupArgs{
			RetentionInDays: pulumi.Int(14),
		})
		if err != nil {
			return err
		}

		lambdaFn, err := lambda.NewFunction(ctx, lambdaName, &lambda.FunctionArgs{
			Code:    pulumi.NewFileArchive("./dist/amd64-lambda.zip"),
			Role:    qaccess.Arn,
			Handler: pulumi.String("amd64-lambda"),
			Runtime: pulumi.String("go1.x"),
			Environment: &lambda.FunctionEnvironmentArgs{
				Variables: pulumi.StringMap{
					// "VERSION":       pulumi.String(version),
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
		// ctx.Export("userAccessKeyId", testVolumeCredentials.ID())
		// ctx.Export("secretKey", testVolumeCredentials.EncryptedSecret)
		return nil
	})
	os.Exit(0)
}
