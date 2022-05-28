package frontend

// import (
// 	"github.com/aws/aws-sdk-go-v2/service/s3"
// 	config "github.com/mabels/s3-streaming-lister/config"
// 	myq "github.com/mabels/s3-streaming-lister/my-queue"
// )

// func HybridStrategy(app *config.S3StreamingLister, pa *PrefixAction, chi myq.MyQueue) {
// 	if pa.Letter {
// 		SingleLetterStrategy(app, &PrefixAction{
// 			ListObjectsV2Input: s3.ListObjectsV2Input{
// 				MaxKeys:   int32(*app.Config.MaxKeys),
// 				Delimiter: app.Config.Delimiter,
// 				Prefix:    &pa.Prefix,
// 				Bucket:    app.Config.Bucket,
// 			},
// 		}, chi)
// 	} else {
// 		DelimiterStrategy(app, &PrefixAction{
// 			ListObjectsV2Input: s3.ListObjectsV2Input{
// 				MaxKeys:   int32(*app.Config.MaxKeys),
// 				Delimiter: app.Config.Delimiter,
// 				Prefix:    &pa.Prefix,
// 				Bucket:    app.Config.Bucket,
// 			},
// 			Letter: true,
// 		}, chi)

// 	}
// }
