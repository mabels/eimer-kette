package timedworker

import (
	"fmt"
)

type Transaction struct {
	// cmd           string
	transaction string
	ResolvFn    func(ce *Transaction, ctx interface{})
	myIndex     int
	protocol    *TransactionProtocol
}

func (result *Transaction) Clean() error {
	if result.myIndex < 0 {
		return fmt.Errorf("not active")
	}
	result.protocol.sync.Lock()
	defer result.protocol.sync.Unlock()
	_, ok := result.protocol.transactions[result.transaction]
	if ok {
		result.protocol.transactions[result.transaction][result.myIndex] = nil
		result.myIndex = -1
		for _, t := range result.protocol.transactions[result.transaction] {
			if t != nil {
				return nil
			}
		}
		delete(result.protocol.transactions, result.transaction)
	} else {
		return fmt.Errorf("transaction not found")
	}
	return nil
}
