package frontend

import "github.com/aws/aws-sdk-go-v2/service/s3/types"

type Complete struct {
	Completed bool
	Todo      []types.Object
}
