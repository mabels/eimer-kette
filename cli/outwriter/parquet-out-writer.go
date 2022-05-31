package outwriter

import (
	"context"
	"io"
	"os"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/reactivex/rxgo/v2"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"

	"github.com/mabels/eimer-kette/cli/config"
	"github.com/mabels/eimer-kette/cli/models"
	"github.com/mabels/eimer-kette/cli/status"
	myq "github.com/mabels/eimer-kette/cli/my-queue"
)

func toEimerKetteItem(obj *types.Object) models.EimerKetteItem {
	// displayName := ""
	// id := ""
	// if obj.Owner != nil {
	// 	if obj.Owner.DisplayName != nil {
	// 		displayName = *obj.Owner.DisplayName
	// 	}
	// 	if obj.Owner.DisplayName != nil {
	// 		id = *obj.Owner.ID
	// 	}
	// }
	return models.EimerKetteItem{
		ETag:         *obj.ETag,
		Key:          *obj.Key,
		LastModified: obj.LastModified.UnixMilli(),
		// Owner: models.EimerKetteOwner{
		// 	DisplayName: displayName,
		// 	ID:          id,
		// },
		Size:         obj.Size,
		StorageClass: string(obj.StorageClass),
	}
}

type ParquetOutWriter struct {
	output             io.Writer
	outputClose        *os.File
	pw                 *writer.ParquetWriter
	chStatus           myq.MyQueue
	waitStreamClosed   sync.Mutex
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

	ow.waitStreamClosed.Lock()
	observable := rxgo.FromChannel(ow.typesObjectChannel).
		BufferWithCount(*ow.app.Config.Output.Parquet.ChunkSize).
		Map(
			func(_ context.Context, items interface{}) (interface{}, error) {
				started := time.Now()
				for _, item := range items.([]interface{}) {
					obj := item.(types.Object)
					ow.app.Clients.Calls.Total.Inc("parquet-write")
					tek := toEimerKetteItem(&obj)
					// fmt.Fprintf(os.Stderr, "tek:%v\n", tek)
					err := ow.pw.Write(tek)
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
		for range observable.Observe() {
		}
		ow.waitStreamClosed.Unlock()
	}()

	return ow
}

func (ow *ParquetOutWriter) write(items *[]types.Object) {
	for _, item := range *items {
		ow.typesObjectChannel <- rxgo.Item{V: item}
	}
}

func (ow *ParquetOutWriter) done() {
	close(ow.typesObjectChannel)
	ow.waitStreamClosed.Lock()
	// fmt.Fprintln(os.Stderr, "done:parquet")
	if err := ow.pw.WriteStop(); err != nil {
		ow.chStatus.Push(status.RunStatus{Err: &err})
	}
	if ow.outputClose != nil {
		ow.outputClose.Close()
	}
}

func makeParquetOutWriter(app *config.S3StreamingLister, chStatus myq.MyQueue) OutWriter {
	fd := app.Output.FileStream
	var outputClose *os.File
	if *app.Config.Output.Parquet.FileName != "" {
		var err error
		outputClose, err = os.Create(*app.Config.Output.Parquet.FileName)
		if err != nil {
			chStatus.Push(status.RunStatus{Fatal: &err})
		}
		fd = outputClose
	}
	ret := &ParquetOutWriter{
		output:             fd,
		outputClose:        outputClose,
		app:                app,
		typesObjectChannel: make(chan rxgo.Item, *app.Config.Output.Parquet.Workers**app.Config.Output.Parquet.ChunkSize),
		waitStreamClosed:   sync.Mutex{},
		chStatus:           chStatus,
	}
	return ret
}
