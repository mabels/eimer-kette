package main

import (
	"encoding/json"
	"errors"
	"reflect"
	"sync"
)

type Chunky struct {
	maxSize     int
	frame       interface{}
	frameSize   int
	currentSize int
	records     chan interface{}
	chunked     int
	chunkedFn   func(c *Chunky, collect int)
	mutex       sync.Mutex
}

func (chunky *Chunky) done(collect int) error {
	if chunky.frameSize < chunky.currentSize {
		// len := len(chunky.records)
		// myrecs := chunky.records[0:len]
		// chunky.records = []interface{}{}
		my := collect
		if my < 0 {
			my = len(chunky.records)
		}
		if my != 0 {
			chunky.chunkedFn(chunky, my)
		}
	}
	return nil
}

func (chunky *Chunky) append(rowOrRows interface{}) error {
	typ := reflect.ValueOf(rowOrRows)
	// fmt.Fprintln(os.Stderr, "Type:", rowOrRows, typ.Type(), typ.Type().Kind())
	switch typ.Type().Kind() {
	case reflect.Slice:
		// case reflect.Array:
		len := typ.Len()
		// fmt.Fprintln(os.Stderr, "Is Array:", len)
		for i := 0; i < len; i++ {
			chunky.append(typ.Index(i).Interface())
		}
	default:
		jsonBytes, err := json.Marshal(rowOrRows)
		// fmt.Println(string(jsonBytes))
		if err != nil {
			return err
		}
		chunky.mutex.Lock()
		// fmt.Fprintf(os.Stderr, "Is Value:%T,%d,%s\n", rowOrRows, len(jsonBytes), string(jsonBytes))
		recordLen := len(jsonBytes)
		if chunky.chunked > 0 {
			recordLen += 1 // comma
		}
		if recordLen >= chunky.maxSize {
			return errors.New("row is bigger than maxSize")
		}
		// fmt.Fprintln(os.Stderr, "xxxx", chunky.currentSize, len(jsonBytes), chunky.maxSize)
		needDone := 0
		if chunky.currentSize+recordLen >= chunky.maxSize {
			needDone = chunky.chunked
			chunky.chunked = 0
			chunky.currentSize = chunky.frameSize
		}
		chunky.chunked++
		chunky.currentSize += recordLen
		chunky.records <- rowOrRows
		chunky.mutex.Unlock()
		chunky.done(needDone)
	}
	return nil
}

func makeChunky(frame interface{}, maxSize int, chunkedFn ...func(c *Chunky, collect int)) (Chunky, error) {
	jsonBytes, err := json.Marshal(frame)
	if len(chunkedFn) == 0 {
		chunkedFn = []func(c *Chunky, co int){func(_ *Chunky, _ int) {}}
	}
	chunky := Chunky{
		maxSize:     maxSize,
		frame:       frame,
		frameSize:   len(jsonBytes),
		currentSize: len(jsonBytes),
		chunkedFn:   chunkedFn[0],
		records:     make(chan interface{}, maxSize/10), // improve
		mutex:       sync.Mutex{},
	}
	if err != nil {
		return chunky, err
	}
	if len(jsonBytes) >= maxSize {
		return chunky, errors.New("frame is bigger than maxSize")
	}
	return chunky, nil
}
