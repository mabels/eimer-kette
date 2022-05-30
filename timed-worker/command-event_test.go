package timedworker

import (
	"fmt"
	"testing"
)

func TestCreateAddNull(t *testing.T) {
	ce := NewCommandEvents(0)
	result, err := ce.Add("cmd1", "transaction1", func(ctx interface{}) {})
	if err == nil {
		t.Errorf("this should have failed")
	}
	if result != nil {
		t.Errorf("result should be nil")
	}
}

func TestCreateAddTwo(t *testing.T) {
	ce := NewCommandEvents(2)
	result, err := ce.Add("cmd1", "transaction1", func(ctx interface{}) {})
	if err != nil {
		t.Errorf("this should have succeeded")
	}
	if result == nil {
		t.Errorf("result should not be nil")
	}
	result, err = ce.Add("cmd1", "transaction1", func(ctx interface{}) {})
	if err != nil {
		t.Errorf("this should have succeeded")
	}
	if result == nil {
		t.Errorf("result should not be nil")
	}
	result, err = ce.Add("cmd1", "transaction1", func(ctx interface{}) {})
	if err == nil {
		t.Errorf("this should have failed")
	}
	if result != nil {
		t.Errorf("result should be nil")
	}
}

func TestCreateAddCleanSimple(t *testing.T) {
	ce := NewCommandEvents(4)
	result, err := ce.Add("cmd1", "transaction1", func(ctx interface{}) {})
	if err != nil {
		t.Errorf("this should have succeeded")
	}
	if result == nil {
		t.Errorf("result should not be nil")
	}
	err = result.Clean()
	if err != nil {
		t.Errorf("this should have succeeded")
	}
	err = result.Clean()
	if err == nil {
		t.Errorf("this should have failed")
	}
}

func TestCreateAddCleanConcurrent(t *testing.T) {
	ce := NewCommandEvents(4)
	waitDone := make(chan bool)
	for i := 0; i < 4; i++ {
		myI := i
		go func() {
			for j := 0; j < 1000; j++ {
				result, err := ce.Add("cmd", fmt.Sprintf("transaction%d-%d", myI, j), func(ctx interface{}) {})
				if err != nil {
					t.Errorf("this should have succeeded")
				}
				if result == nil {
					t.Errorf("result should not be nil")
					continue
				}
				err = result.Clean()
				if err != nil {
					t.Errorf("this should have succeeded")
				}
			}
			waitDone <- true
		}()
	}
	for i := 0; i < 4; i++ {
		<-waitDone
	}
}

func TestFindCommand(t *testing.T) {
	ce := NewCommandEvents(4)
	my, err := ce.Find("cmd", "transaction")
	if err != nil {
		t.Errorf("this should have succeeded")
	}
	if my != nil {
		t.Errorf("result should be nil")
	}
	my, err = ce.Add("cmd", "transaction", func(ctx interface{}) {})
	if err != nil {
		t.Errorf("this should have succeeded")
	}
	my, err = ce.Find("cmd", "transaction")
	if err != nil {
		t.Errorf("this should have succeeded")
	}
	if my == nil {
		t.Errorf("result should not be nil")
	}
	my, err = ce.Find("cmd", "transaction")
	if err != nil {
		t.Errorf("this should have succeeded")
	}
	if my == nil {
		t.Errorf("result should not be nil")
	}
	err = my.Clean()
	if err != nil {
		t.Errorf("this should have succeeded")
	}
	my, err = ce.Find("cmd", "transaction")
	if err != nil {
		t.Errorf("this should have succeeded")
	}
	if my != nil {
		t.Errorf("result should be nil")
	}

}
