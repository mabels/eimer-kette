package main

import (
	"encoding/json"
	"errors"
)

type Chunky struct {
	maxSize     int
	frame       interface{}
	frameSize   int
	currentSize int
	records   []interface{}
	chunkedFn func(c Chunky)
}

func (chunky *Chunky) done() error {
	if chunky.frameSize < chunky.currentSize {
		chunky.chunkedFn(*chunky)
		chunky.records = []interface{}{}
		chunky.currentSize = chunky.frameSize
	}
	return nil
}

func (chunky *Chunky) append(row interface{}) error {
	jsonBytes, err := json.Marshal(row)
	// fmt.Println(string(jsonBytes))
	if err != nil {
		return err
	}
	recordLen := len(jsonBytes)
	if len(chunky.records) != 0 {
		recordLen += 1 // comma
	}
	if recordLen >= chunky.maxSize {
		return errors.New("row is bigger than maxSize")
	}
	// fmt.Fprintln(os.Stderr, "xxxx", chunky.currentSize, len(jsonBytes), chunky.maxSize)
	if chunky.currentSize + recordLen >= chunky.maxSize {
		chunky.done()
	}
	chunky.currentSize += recordLen
	chunky.records = append(chunky.records, row)
	return nil
}

func makeChunky(frame interface{}, maxSize int, chunkedFn func(c Chunky)) (Chunky, error) {
	jsonBytes, err := json.Marshal(frame)
	chunky := Chunky{
		maxSize:     maxSize,
		frame:       frame,
		frameSize:   len(jsonBytes),
		currentSize: len(jsonBytes),
		chunkedFn:   chunkedFn,
	}
	if err != nil {
		return chunky, err
	}
	if len(jsonBytes) >= maxSize {
		return chunky, errors.New("frame is bigger than maxSize")
	}
	return chunky, nil
}
