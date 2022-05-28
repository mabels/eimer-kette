package frontend

// import (
// 	"sync/atomic"

// 	"github.com/aws/aws-sdk-go-v2/service/s3"
// 	config "github.com/mabels/s3-streaming-lister/config"
// 	myq "github.com/mabels/s3-streaming-lister/my-queue"
// )

// func SingleLetterStrategy(app *config.S3StreamingLister, pa *PrefixAction, chi myq.MyQueue) {
// 	// atomic.AddInt32(&app.InputConcurrent, int32(len(*app.Config.Prefixes)))
// 	for _, letter := range *app.Config.Prefixes {
// 		nextPrefix := pa.Prefix + letter
// 		atomic.AddInt32(&app.InputConcurrent, 1)
// 		app.Clients.Calls.Total.Inc("ListObjectsV2Input")
// 		app.Clients.Calls.Concurrent.Inc("ListObjectsV2Input")
// 		chi.Push(&PrefixAction{
// 			ListObjectsV2Input: s3.ListObjectsV2Input{
// 				MaxKeys: int32(*app.Config.MaxKeys),
// 				// Delimiter: &string{}, // app.Config.Delimiter,
// 				Prefix: &nextPrefix,
// 				Bucket: app.Config.Bucket,
// 			},
// 			Prefix: nextPrefix,
// 		})
// 	}
// }
