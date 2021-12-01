package main

import (
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type AwsLsOutWriter struct {
	output io.Writer
}

func (ow *AwsLsOutWriter) setup() OutWriter {
	return ow
}

func (ow *AwsLsOutWriter) write(items *[]types.Object) {
	for _, item := range *items {
		fmt.Fprintf(ow.output, "%s %10d %s\n",
			item.LastModified.Format("2006-01-02 15:04:05"), item.Size, *item.Key)
	}
}

func (ow *AwsLsOutWriter) done() {
}

func makeAwsLsOutWriter(out io.Writer) OutWriter {
	return &AwsLsOutWriter{
		output: out,
	}
}
