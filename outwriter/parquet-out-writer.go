package outwriter

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/mabels/eimer-kette/config"
	"github.com/mabels/eimer-kette/models"
	myq "github.com/mabels/eimer-kette/my-queue"
	"github.com/mabels/eimer-kette/status"
	"github.com/reactivex/rxgo/v2"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

func toEimerKetteItem(obj *types.Object) models.EimerKetteItem {
	displayName := ""
	id := ""
	if obj.Owner != nil {
		if obj.Owner.DisplayName != nil {
			displayName = *obj.Owner.DisplayName
		}
		if obj.Owner.DisplayName != nil {
			id = *obj.Owner.ID
		}
	}
	return models.EimerKetteItem{
		ETag:         *obj.ETag,
		Key:          *obj.Key,
		LastModified: obj.LastModified.UnixMilli(),
		Owner: models.EimerKetteOwner{
			DisplayName: displayName,
			ID:          id,
		},
		Size:         obj.Size,
		StorageClass: string(obj.StorageClass),
	}
}

type ParquetOutWriter struct {
	output io.Writer
	// np       int64
	pw                 *writer.ParquetWriter
	chStatus           myq.MyQueue
	waitComplete       sync.Mutex
	typesObjectChannel chan rxgo.Item
	app                *config.S3StreamingLister
}

func (ow *ParquetOutWriter) setup() OutWriter {
	// fw, err := local.NewLocalFileWriter("json.parquet")
	// if err != nil {
	// log.Println("Can't create file", err)
	// return
	// }
	var err error
	ow.pw, err = writer.NewParquetWriterFromWriter(ow.output, new(models.EimerKetteItem), int64(*ow.app.Config.Output.Parquet.Workers))
	// pw, err := writer.NewJSONWriter(md, fw, 4)
	if err != nil {
		ow.chStatus.Push(status.RunStatus{Err: &err})
	}
	ow.pw.RowGroupSize = 1024 * 1024 //128M
	ow.pw.CompressionType = parquet.CompressionCodec_SNAPPY

	observable := rxgo.FromChannel(ow.typesObjectChannel).BufferWithCount(*ow.app.Config.Output.Parquet.ChunkSize).Map(
		func(_ context.Context, items interface{}) (interface{}, error) {
			started := time.Now()
			for _, item := range items.([]interface{}) {
				obj := item.(types.Object)
				err := ow.pw.Write(toEimerKetteItem(&obj))
				if err != nil {
					ow.app.Clients.Calls.Error.Inc("parquet-write")
					ow.chStatus.Push(status.RunStatus{Err: &err})
					return nil, err
				}
			}
			ow.app.Clients.Calls.Total.Duration("parquet-writes", started)
			return nil, nil
		},
		rxgo.WithPool(*ow.app.Config.Output.Parquet.Workers),
	)
	go func() {
		// fmt.Fprintln(os.Stderr, "setup-runner-pre")
		for range observable.Observe() {
		}
		// fmt.Fprintln(os.Stderr, "setup-runner-post")
		ow.waitComplete.Unlock()
	}()

	return ow
}

func (ow *ParquetOutWriter) write(items *[]types.Object) {
	for _, item := range *items {
		ow.typesObjectChannel <- rxgo.Item{V: item}
	}
}

func (ow *ParquetOutWriter) done() {
	if err := ow.pw.WriteStop(); err != nil {
		ow.chStatus.Push(status.RunStatus{Err: &err})
		return
	}
	close(ow.typesObjectChannel)
	ow.waitComplete.Lock()
	ow.waitComplete.Unlock()
	// log.Println("Write Finished")
	// ow.output.Close()
}

func makeParquetOutWriter(app *config.S3StreamingLister, chStatus myq.MyQueue) OutWriter {
	ret := &ParquetOutWriter{
		output:             app.Output.FileStream,
		app:                app,
		typesObjectChannel: make(chan rxgo.Item, *app.Config.Output.Parquet.Workers**app.Config.Output.Parquet.ChunkSize),
		waitComplete:       sync.Mutex{},
		chStatus:           chStatus,
	}
	ret.waitComplete.Lock()
	return ret
}
