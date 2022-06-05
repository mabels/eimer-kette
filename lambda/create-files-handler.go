package lambda

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/mabels/eimer-kette/models"
	"github.com/mabels/eimer-kette/status"
	"github.com/reactivex/rxgo/v2"
)

func cleanCreateFilesPayload(cmd *models.CmdCreateFiles) string {
	out := *cmd
	out.Payload.Bucket.Credentials = cleanAwsCredentials(cmd.Payload.Bucket.Credentials)
	return toJsonString(out)
}

func (hctx *HandlerCtx) writeSingleFiles(cmd *models.CmdCreateFiles, started time.Time, bc *BackChannel) {
	// log.Printf("-1-writeSingleFiles:%v", cleanCreateTestPayload(cmd))
	if cmd.Payload.SkipCreate {
		return
	}
	// log.Printf("-2-writeSingleFiles:%v", cleanCreateTestPayload(cmd))

	// todos := map[string]s3.PutObjectInput{}
	todos := make([]s3.PutObjectInput, cmd.Payload.NumberOfFiles)
	// idProvider := uuid.New()
	// log.Printf("2-Single: %v", cleanCreateTestPayload(cmd))
	// log.Printf("-3-writeSingleFiles:%v", cleanCreateTestPayload(cmd))
	for i := 0; i < int(cmd.Payload.NumberOfFiles); i++ {
		id := strings.ReplaceAll(uuid.New().String(), "-", "/")
		todos[i] = s3.PutObjectInput{
			Key:    &id,
			Bucket: &cmd.Payload.Bucket.Name,
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
			if time.Since(started) >= cmd.Payload.ScheduleTime {
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
			// if err != nil {
			// 	bc.SendStatus(status.RunResult{
			// 		Action: "Error",
			// 		Took:   took,
			// 		Err:    err,
			// 	})
			// 	return nil, err
			// }
			if took.Milliseconds() > 100 {
				bc.SendStatus(status.RunResult{
					Action:  "Warning",
					Took:    took,
					Objects: 0,
					Err:     fmt.Errorf("S3putObject: %f:%s", time.Since(started).Seconds(), *item.(s3.PutObjectInput).Key),
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
		rxgo.WithPool(cmd.Payload.JobConcurrent),
	).Reduce(func(_ context.Context, acc interface{}, item interface{}) (interface{}, error) {
		if acc == nil {
			acc = *cmd
		}
		ocmd := acc.(models.CmdCreateFiles)
		result := item.(Result)
		if result.Error != nil || result.Result != nil {
			ocmd.Payload.NumberOfFiles--
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
		bc.SendStatus(status.RunResult{Action: "Error", Err: fmt.Errorf("writeSingleFiles:rxgo: %v", err)})
		return
	}
	// log.Printf("-10-writeSingleFiles:%v", cleanCreateTestPayload(cmd))
	rcmd := ritem.V.(models.CmdCreateFiles)
	hctx.pushJobSizeCommands(&rcmd, rcmd.Payload.NumberOfFiles, started, bc)
	took := time.Since(started)
	// bc.SendStatus(status.RunResult{
	log.Printf("writeSingleFiles:took %f - %d of %d:%v", took.Seconds(),
		cmd.Payload.NumberOfFiles-rcmd.Payload.NumberOfFiles,
		cmd.Payload.NumberOfFiles,
		cleanCreateFilesPayload(&rcmd))
}

func (hctx *HandlerCtx) pushJobSizeCommands(cmd *models.CmdCreateFiles, jobSize int64, started time.Time, bc *BackChannel) {
	pushStarted := time.Now()
	for done := int64(0); done < cmd.Payload.NumberOfFiles; done += jobSize {
		if time.Since(started) > cmd.Payload.ScheduleTime {
			cmd.Payload.NumberOfFiles = cmd.Payload.NumberOfFiles - done
			// log.Printf("pushJobSizeCommands:reschedule: %v", cleanCreateTestPayload(cmd))
			_, err := hctx.SqsClient.SendMessageJson(cmd)
			if err != nil {
				log.Printf("SQS-SendMessage: %v:%v", hctx.QueueUrl, err)
			}
			bc.SendMessageJson(status.RunResult{
				Action:  "pushJobSizeCommands",
				Took:    time.Since(pushStarted),
				Objects: done,
			})
			log.Printf("pushJobSizeCommands:took %f", time.Since(started).Seconds())
			return
		}
		my := *cmd
		if done+jobSize > cmd.Payload.NumberOfFiles {
			my.Payload.NumberOfFiles = cmd.Payload.NumberOfFiles - done
		} else {
			my.Payload.NumberOfFiles = jobSize
		}
		// log.Printf("pushSingleCommands:%d of %d:%v", done, cmd.Payload.NumberOfFiles, cleanCreateTestPayload(&my))
		_, err := hctx.SqsClient.SendMessageJson(my)
		if err != nil {
			log.Printf("SQS-SendMessage: %v:%v", hctx.QueueUrl, err)
		}
	}
}

func (hctx *HandlerCtx) createTestHandler(cmd *models.CmdCreateFiles, started time.Time, bc *BackChannel) {
	parts := cmd.Payload.NumberOfFiles / int64(cmd.Payload.JobSize)
	if parts <= 1 {
		// log.Printf("Single: %v", cleanCreateTestPayload(cmd))
		hctx.writeSingleFiles(cmd, started, bc)
	} else if parts <= int64(cmd.Payload.JobSize) {
		// log.Printf("Push-1: %v", cleanCreateTestPayload(cmd))
		hctx.pushJobSizeCommands(cmd, int64(cmd.Payload.JobSize), started, bc)
	} else if parts > int64(cmd.Payload.JobSize) {
		// log.Printf("Push-2: %v", cleanCreateTestPayload(cmd))
		hctx.pushJobSizeCommands(cmd, int64(parts), started, bc)
	}
}
