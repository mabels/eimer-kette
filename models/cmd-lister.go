package models

import "time"

type ListerPayload struct {
	Bucket       BucketParams  `json:"Bucket"`
	ScheduleTime time.Duration `json:"ScheduleTime"` // default 2000msec
	BackChannel  SqsParams     `json:"BackChannel"`
}

type CmdLister struct {
	Command string `json:"Command"`
	Payload ListerPayload
}
