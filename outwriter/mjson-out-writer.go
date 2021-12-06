package outwriter

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type MjsonOutWriter struct {
	output io.Writer
}

func (ow *MjsonOutWriter) setup() OutWriter {
	return ow
}

func (ow *MjsonOutWriter) write(items *[]types.Object) {
	for _, item := range *items {
		out, _ := json.Marshal(item)
		fmt.Fprintln(ow.output, string(out))
	}
}

func (ow *MjsonOutWriter) done() {

}

func makeMjsonOutWriter(out io.Writer) OutWriter {
	return &MjsonOutWriter{
		output: out,
	}
}
