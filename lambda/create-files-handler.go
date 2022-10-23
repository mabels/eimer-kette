package lambda

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/fatih/structs"
	"github.com/google/uuid"
	"github.com/mabels/eimer-kette/models"
	"github.com/mabels/eimer-kette/status"
	"github.com/reactivex/rxgo/v2"
)

func cleanCreateFilesPayload(cmd *models.CreateFilesPayload) *map[string]interface{} {
	out := structs.Map(*cmd)

	return cleanAwsCredentials(&out)
	// return toJsonString(out)
}

func (hctx *HandlerCtx) writeSingleFiles(cmd *models.CreateFilesPayload, started time.Time, bc *BackChannel) {
	if cmd.SkipCreate {
		return
	}
	// log.Printf("-2-writeSingleFiles:%v", cleanCreateTestPayload(cmd))

	// todos := map[string]s3.PutObjectInput{}
	todos := make([]s3.PutObjectInput, cmd.NumberOfFiles)
	// idProvider := uuid.New()
	// log.Printf("2-Single: %v", cleanCreateTestPayload(cmd))
	// log.Printf("-3-writeSingleFiles:%v", cleanCreateTestPayload(cmd))
	for i := 0; i < int(cmd.NumberOfFiles); i++ {
		id := strings.ReplaceAll(uuid.New().String(), "-", "/")
		todos[i] = s3.PutObjectInput{
			Key:    &id,
			Bucket: &cmd.Bucket.Name,
			Body:   strings.NewReader(id),
		}
	}
	// log.Printf("-4-writeSingleFiles:%v", cleanCreateTestPayload(cmd))
	// log.Printf("2-Single: %v", cleanCreateTestPayload(cmd))
	//until := started.Add(cmd.Payload.ScheduleTime)
	observable := rxgo.Just(todos)().Map(
		func(_ context.Context, item interface{}) (interface{}, error) {
			// log.Printf("-5-writeSingleFiles:%v", cleanCreateTestPayload(cmd))
			// log.Printf("3-Single: %v:%v", cleanCreateTestPayload(cmd), items)
			// result := make([]Result, 0, len(items.([]interface{})))
			// log.Printf("4-Single: %v:%d", cleanCreateTestPayload(cmd), len(items.([]interface{})))
			// log.Printf("3-Single: %v", item)
			// for _, item := range items.([]interface{}) {
			if time.Since(started) >= cmd.ScheduleTime {
				// log.Printf("4-Single: %v", item)
				// log.Printf("-6-writeSingleFiles:%v", cleanCreateTestPayload(cmd))
				return Result{
					Request: item.(s3.PutObjectInput),
				}, nil
			}
			now := time.Now()
			// log.Printf("Write: %v", item.(s3.PutObjectInput).Key)
			out, err := hctx.S3putObject(cmd, item.(s3.PutObjectInput))
			took := time.Since(now)

			if err != nil {
				bc.SendResult(status.RunResult{
					Action:  "Error:S3putObject",
					Took:    took,
					Related: item.(s3.PutObjectInput),
					Err:     err,
					Fatal:   nil,
				})
			} else if took.Seconds() > 0.1 {
				bc.SendResult(status.RunResult{
					Action:  "Warning:S3putObject",
					Took:    took,
					Related: item.(s3.PutObjectInput),
					Warning: aws.String(fmt.Sprintf("%f:%s", time.Since(started).Seconds(), *item.(s3.PutObjectInput).Key)),
					Fatal:   nil,
				})
			}
			result := Result{
				Request: item.(s3.PutObjectInput),
				Result:  out,
				Error:   err,
			}
			// log.Printf("-7-writeSingleFiles:%v", cleanCreateTestPayload(cmd))
			// log.Printf("5.1-Single: %v:%d", cleanCreateTestPayload(cmd), time.Since(now))
			return result, nil
		},
		rxgo.WithPool(cmd.JobConcurrent),
	).Reduce(func(_ context.Context, acc interface{}, item interface{}) (interface{}, error) {
		if acc == nil {
			acc = *cmd
		}
		ocmd := acc.(models.CreateFilesPayload)
		result := item.(Result)
		if result.Error != nil || result.Result != nil {
			ocmd.NumberOfFiles--
			// log.Printf("6.1-Single: %v:%v", ocmd, item)
			// delete(todos, *result.Request.Key)
		}
		// log.Printf("-8-writeSingleFiles:%v", cleanCreateTestPayload(cmd))
		return ocmd, nil
	})

	// log.Printf("-9-writeSingleFiles:%v", cleanCreateTestPayload(cmd))
	// log.Printf("7-Single:")
	ritem, err := observable.Get()
	// log.Printf("8-Single: %v:%v", ritem, err)
	if err != nil {
		// log.Printf("PutObjects:Error %v", err)
		bc.SendResult(status.RunResult{
			Action: "Error:RxGo",
			Err:    err,
		})
		return
	}
	// log.Printf("-10-writeSingleFiles:%v", cleanCreateTestPayload(cmd))
	rcmd := ritem.V.(models.CreateFilesPayload)
	hctx.pushJobSizeCommands(&rcmd, rcmd.NumberOfFiles, started, bc)
	took := time.Since(started)
	// bc.SendStatus(status.RunResult{
	infoStr := fmt.Sprintf("took %f - %d of %d", took.Seconds(),
		cmd.NumberOfFiles-rcmd.NumberOfFiles,
		cmd.NumberOfFiles)
	bc.SendResult(status.RunResult{
		Action:  "Info:writeSingleFiles",
		Info:    &infoStr,
		Related: cleanCreateFilesPayload(&rcmd),
	})
}

func (hctx *HandlerCtx) pushJobSizeCommands(cmd *models.CreateFilesPayload, jobSize int64, started time.Time, bc *BackChannel) {
	pushStarted := time.Now()
	for done := int64(0); done < cmd.NumberOfFiles; done += jobSize {
		if time.Since(started) > cmd.ScheduleTime {
			cmd.NumberOfFiles = cmd.NumberOfFiles - done
			// log.Printf("pushJobSizeCommands:reschedule: %v", cleanCreateTestPayload(cmd))
			_, err := hctx.SqsClient.SendCmdCreateFiles(cmd)
			if err != nil {
				bc.SendResult(status.RunResult{
					Action:  "Error:SendCmdCreateFiles:pushJobSizeCommands",
					Took:    time.Since(pushStarted),
					Related: cleanCreateFilesPayload(cmd),
					Err:     err,
				})
			}
			bc.SendResult(status.RunResult{
				Action:  "Info:Reschedule:pushJobSizeCommands",
				Took:    time.Since(pushStarted),
				Related: cleanCreateFilesPayload(cmd),
			})
			// log.Printf("pushJobSizeCommands:took %f", time.Since(started).Seconds())
			return
		}
		my := *cmd
		if done+jobSize > cmd.NumberOfFiles {
			my.NumberOfFiles = cmd.NumberOfFiles - done
		} else {
			my.NumberOfFiles = jobSize
		}
		_, err := hctx.SqsClient.SendCmdCreateFiles(&my)
		if err != nil {
			bc.SendResult(status.RunResult{
				Action:  "Error:SendCmdCreateFiles:pushJobSizeCommands",
				Took:    time.Since(pushStarted),
				Related: cleanCreateFilesPayload(&my),
				Err:     err,
			})
		}
	}
}

func (hctx *HandlerCtx) createTestHandler(cmd *models.CreateFilesPayload, started time.Time, bc *BackChannel) {
	parts := cmd.NumberOfFiles / int64(cmd.JobSize)
	if parts <= 1 {
		// log.Printf("Single: %v", cleanCreateTestPayload(cmd))
		hctx.writeSingleFiles(cmd, started, bc)
	} else if parts <= int64(cmd.JobSize) {
		// log.Printf("Push-1: %v", cleanCreateTestPayload(cmd))
		hctx.pushJobSizeCommands(cmd, int64(cmd.JobSize), started, bc)
	} else if parts > int64(cmd.JobSize) {
		// log.Printf("Push-2: %v", cleanCreateTestPayload(cmd))
		hctx.pushJobSizeCommands(cmd, int64(parts), started, bc)
	}
}
