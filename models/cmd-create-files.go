package models

import "time"

type CreateFilesPayload struct {
	Bucket        BucketParams  `json:"Bucket"`
	NumberOfFiles int64         `json:"NumberOfFiles"`
	JobSize       int           `json:"JobSize"`
	JobConcurrent int           `json:"JobConcurrent"`
	SkipCreate    bool          `json:"SkipCreate"`
	ScheduleTime  time.Duration `json:"ScheduleTime"` // default 2000msec
	BackChannel   SqsParams     `json:"BackChannel"`
}

type CmdCreateFiles struct {
	Command string `json:"Command"`
	Payload CreateFilesPayload
}
