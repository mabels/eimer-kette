package timedworker

import (
	"fmt"
	"testing"
)

func TestCreateAddNull(t *testing.T) {
	ce := NewTransactionProtocol(0)
	result, err := ce.Add("transaction1", func(ce *Transaction, ctx interface{}) {})
	if err != nil {
		t.Errorf("this should have failed:%v", err)
	}
	if result == nil {
		t.Errorf("result should be nil")
	}
}

func TestCreateAddTwo(t *testing.T) {
	ce := NewTransactionProtocol(2)
	result, err := ce.Add("transaction1", func(ce *Transaction, ctx interface{}) {})
	if err != nil {
		t.Errorf("this should have succeeded")
	}
	if result == nil {
		t.Errorf("result should not be nil")
	}
	if len(ce.transactions["transaction1"]) != 1 {
		t.Errorf("result should not be len 1")
	}
	result, err = ce.Add("transaction1", func(ce *Transaction, ctx interface{}) {})
	if err != nil {
		t.Errorf("this should have succeeded:%v", err)
	}
	if result == nil {
		t.Errorf("result should not be nil")
	}
	if len(ce.transactions["transaction1"]) != 2 {
		t.Errorf("result should not be len 2")
	}
	result, err = ce.Add("transaction1", func(ce *Transaction, ctx interface{}) {})
	if err != nil {
		t.Errorf("this should have failed")
	}
	if result == nil {
		t.Errorf("result should be nil")
	}
	if len(ce.transactions["transaction1"]) != 3 {
		t.Errorf("result should not be len 3")
	}

}

func TestCreateAddCleanSimple(t *testing.T) {
	ce := NewTransactionProtocol(4)
	result, err := ce.Add("transaction1", func(ce *Transaction, ctx interface{}) {})
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
	if len(result.protocol.transactions) != 0 {
		t.Errorf("clean should remove key if empty")
	}
	err = result.Clean()
	if err == nil {
		t.Errorf("this should have failed")
	}
}

func TestCreateAddCleanConcurrent(t *testing.T) {
	ce := NewTransactionProtocol(4)
	waitDone := make(chan bool)
	for i := 0; i < 4; i++ {
		myI := i
		go func() {
			for j := 0; j < 1000; j++ {
				result, err := ce.Add(fmt.Sprintf("transaction%d-%d", myI, j), func(ce *Transaction, ctx interface{}) {})
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
	ce := NewTransactionProtocol(4)
	mys, err := ce.Find("transaction")
	if err != nil {
		t.Errorf("this should have succeeded")
	}
	if len(mys) != 0 {
		t.Errorf("result should be empty")
	}
	my, err := ce.Add("transaction", func(ce *Transaction, ctx interface{}) {})
	if err != nil {
		t.Errorf("this should have succeeded")
	}
	mys, err = ce.Find("transaction")
	if err != nil {
		t.Errorf("this should have succeeded")
	}
	if len(mys) != 1 {
		t.Errorf("result should not be nil:%v", my)
	}
	err = my.Clean()
	if err != nil {
		t.Errorf("this should have succeeded")
	}
	mys, err = ce.Find("transaction")
	if err != nil {
		t.Errorf("this should have succeeded")
	}
	if len(mys) != 0 {
		t.Errorf("result should be empty")
	}

}

func TestTransactionEmpty(t *testing.T) {
	ce := NewTransactionProtocol(4)
	mys, err := ce.Find("transaction")
	if err != nil {
		t.Errorf("this should have succeeded")
	}
	if len(mys) != 0 {
		t.Errorf("result should be empty")
	}
	my, err := ce.Add("Bla", func(ce *Transaction, ctx interface{}) {})
	if err != nil {
		t.Errorf("this should have succeeded")
	}
	if my == nil {
		t.Errorf("this should have my")
	}
	mys, err = ce.Find("")
	if err != nil {
		t.Errorf("this should have succeeded")
	}
	if len(mys) != 0 {
		t.Errorf("this should find my")
	}
}
