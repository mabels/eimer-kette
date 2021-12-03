package main

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"testing"
)

type Cell struct {
	Id string
}

type Frame struct {
	Id   string
	Rows []Cell
}

func TestEmptyArray(t *testing.T) {
	frame := Frame{Id: "Test", Rows: []Cell{}}
	_, err := makeChunky(frame, 4, func(c *Chunky, _ int) {
		t.Error("Should not Called")
	})
	if err == nil {
		t.Error("should be to short")
	}
	chunky, err := makeChunky(&frame, 1000, func(c *Chunky, _ int) {
		t.Error("Should not Called")
	})
	if err != nil {
		t.Error("should be cause an error")
	}
	if chunky.frameSize == 0 {
		t.Error("frameSize should be set")
	}
}

func TestRowToBig(t *testing.T) {
	frame := Frame{Id: "Test", Rows: []Cell{}}
	chunky, _ := makeChunky(&frame, 1000, func(c *Chunky, _ int) {
		t.Error("Should not Called")
	})
	test := make([]byte, 10000)
	for j := range test {
		test[j] = 66
	}
	err := chunky.append(Cell{Id: string(test)})
	if err == nil {
		t.Error("should be an error")
	}
	chunky.done(-1)
}

func TestLowFill(t *testing.T) {
	frame := Frame{Id: "Test", Rows: []Cell{}}
	chunkedRef := []int{10}
	chunky, _ := makeChunky(&frame, 1000, func(c *Chunky, collect int) {
		fmt.Fprintln(os.Stderr, "XXXXX", collect)
		if collect != chunkedRef[0] {
			t.Error("Should not Called", collect)
		}
		for i := 0; i < collect; i++ {
			<-c.records
		}
		chunkedRef = chunkedRef[1:]
	})
	test := make([]byte, 6)
	for j := range test {
		test[j] = 66
	}
	for i := 0; i < 10; i += 1 {
		err := chunky.append(Cell{Id: string(test)})
		if err != nil {
			t.Error("should be an error")
		}
	}
	chunky.done(-1)
	if len(chunkedRef) != 0 {
		t.Error("chunkedRef")
	}
	chunky.done(-1)
}

func TestChunkedFill(t *testing.T) {
	frame := Frame{Id: "Test", Rows: []Cell{}}
	chunkedRef := []int{66, 64, 64, 61, 61, 60, 60, 60, 60, 60, 60, 60,
		60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60,
		60, 60, 60, 60, 60, 60, 60, 4,
	}
	sum := 0
	for _, i := range chunkedRef {
		sum += i
	}
	if sum != 2000 {
		t.Error("there should be chunks for 2000=", sum, len(chunkedRef))
	}
	records := 0
	chunky, _ := makeChunky(&frame, 1000, func(c *Chunky, collect int) {
		// t.Error("XXXXX:", len(c.records))
		if collect != chunkedRef[0] {
			t.Error("Should not Called", len(c.records))
		}
		cframe := c.frame.(*Frame)
		cframe.Rows = []Cell{}
		for i := 0; i < collect; i++ {
			cframe.Rows = append(cframe.Rows, Cell{
				Id: reflect.ValueOf(<-c.records).FieldByName("Id").String(),
			})
		}
		jsonBytes, _ := json.Marshal(cframe)
		if len(jsonBytes) < c.frameSize || len(jsonBytes) >= c.maxSize {
			t.Error(fmt.Sprintf("size error:jsonBytes=%d < c.frameSize=%d || jsonBytes=%d >= c.maxSize=%d", len(jsonBytes), c.frameSize, len(jsonBytes), c.maxSize))
		}
		records += collect
		chunkedRef = chunkedRef[1:]
	})
	test := make([]byte, 6)
	for j := range test {
		test[j] = 66
	}
	for i := 0; i < 1000; i += 1 {
		// t.Error("i=", i)
		cells := []Cell{{Id: fmt.Sprintf("%s%d", string(test), i)}, {Id: fmt.Sprintf("%d", i)}}
		err := chunky.append(cells)
		if err != nil {
			t.Error("should be an error")
		}
	}
	chunky.done(-1)
	if records != 2000 {
		t.Error("records=", records)
	}
	if len(chunkedRef) != 0 {
		t.Error("chunkedRef")
	}
}
