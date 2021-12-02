package main

import (
	"encoding/json"
	"errors"
	"reflect"
)

type Chunky struct {
	maxSize     int
	frame       interface{}
	frameSize   int
	currentSize int
	records     []interface{}
	chunkedFn   func(c *Chunky)
}

func (chunky *Chunky) done() error {
	if chunky.frameSize < chunky.currentSize {
		chunky.chunkedFn(chunky)
		chunky.records = []interface{}{}
		chunky.currentSize = chunky.frameSize
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
		// fmt.Fprintf(os.Stderr, "Is Value:%T,%d,%s\n", rowOrRows, len(jsonBytes), string(jsonBytes))
		recordLen := len(jsonBytes)
		if len(chunky.records) != 0 {
			recordLen += 1 // comma
		}
		if recordLen >= chunky.maxSize {
			return errors.New("row is bigger than maxSize")
		}
		// fmt.Fprintln(os.Stderr, "xxxx", chunky.currentSize, len(jsonBytes), chunky.maxSize)
		if chunky.currentSize+recordLen >= chunky.maxSize {
			chunky.done()
		}
		chunky.currentSize += recordLen
		chunky.records = append(chunky.records, rowOrRows)
	}
	return nil
}

func makeChunky(frame interface{}, maxSize int, chunkedFn ...func(c *Chunky)) (Chunky, error) {
	jsonBytes, err := json.Marshal(frame)
	if len(chunkedFn) == 0 {
		chunkedFn = []func(c *Chunky){func(_ *Chunky) {}}
	}
	chunky := Chunky{
		maxSize:     maxSize,
		frame:       frame,
		frameSize:   len(jsonBytes),
		currentSize: len(jsonBytes),
		chunkedFn:   chunkedFn[0],
	}
	if err != nil {
		return chunky, err
	}
	if len(jsonBytes) >= maxSize {
		return chunky, errors.New("frame is bigger than maxSize")
	}
	return chunky, nil
}
