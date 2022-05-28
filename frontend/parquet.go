package frontend

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/reactivex/rxgo/v2"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"

	config "github.com/mabels/eimer-kette/config"
	"github.com/mabels/eimer-kette/models"
	myq "github.com/mabels/eimer-kette/my-queue"
	"github.com/mabels/eimer-kette/status"
)

func Parquet(app *config.S3StreamingLister, cho myq.MyQueue, chStatus myq.MyQueue) {

	fr, err := local.NewLocalFileReader(*app.Config.Frontend.Parquet.Filename)
	if err != nil {
		chStatus.Push(status.RunStatus{Fatal: &err})
		return
	}
	pr, err := reader.NewParquetReader(fr, new(models.EimerKetteItem), int64(*app.Config.Frontend.Parquet.Workers))
	if err != nil {
		chStatus.Push(status.RunStatus{Fatal: &err})
		return
	}
	go func() {
		producer := 0
		obs := rxgo.Defer([]rxgo.Producer{func(_ context.Context, ch chan<- rxgo.Item) {
			producer++
			// fmt.Fprintln(os.Stderr, "Producer:", producer)
			num := int(pr.GetNumRows())
			for i := 0; i < num; {
				// if i%2 == 0 {
				// 	pr.SkipRows(int64(*app.Config.Frontend.Parquet.RowBuffer))
				// 	continue
				// }
				stus := make([]models.EimerKetteItem, *app.Config.Frontend.Parquet.RowBuffer) //read 10 rows
				if err = pr.Read(&stus); err != nil {
					chStatus.Push(status.RunStatus{Err: &err})
				} else {
					app.Clients.Calls.Total.Inc("parquet-read")
					// fmt.Fprintf(os.Stderr, "input:%v\n", stus[0])
					for _, stu := range stus {
						lm := time.UnixMilli(stu.LastModified)
						key := stu.Key
						etag := stu.ETag
						res := types.Object{
							ETag:         &etag,
							Key:          &key,
							LastModified: &lm,
							Size:         stu.Size,
						} // with one element
						ch <- rxgo.Item{V: res}
					}
					i += len(stus)
				}
			}
		}}).BufferWithCount(1000)
		nextCnt := 0
		for item := range obs.Observe() {
			if item.E != nil {
				chStatus.Push(status.RunStatus{Err: &item.E})
			}
			if item.V != nil {
				items := item.V.([]interface{})
				nextCnt += len(items)
				// fmt.Fprintf(os.Stderr, "len:%d:%v\n", len(items), items[0])
				objs := make([]types.Object, 0, len(items))
				for _, item := range items {
					objs = append(objs, item.(types.Object))
				}
				app.Clients.Calls.Total.Add(len(objs), "parquet-out")
				// fmt.Fprintf(os.Stderr, "do-next-pre-post:%v:%v\n", objs[0], items[0])
				cho.Push(Complete{Todo: objs, Completed: false})
				// fmt.Fprintln(os.Stderr, "do-next-post-post")
			}
		}
		// fmt.Fprintln(os.Stderr, "do-complete")
		cho.Push(Complete{Todo: nil, Completed: true})
		pr.ReadStop()
		fr.Close()
		// fmt.Fprintln(os.Stderr, "obs-run-done")
	}()

}
