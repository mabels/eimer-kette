package timedworker

import (
	"sync"
)

type TransactionProtocol struct {
	transactions map[string][]*Transaction
	sync         sync.Mutex
	inFlight     int
}

func NewTransactionProtocol(optional_inFlight ...int) *TransactionProtocol {
	inFlight := 10
	if len(optional_inFlight) > 0 {
		inFlight = optional_inFlight[0]
	}
	return &TransactionProtocol{
		transactions: make(map[string][]*Transaction),
		sync:         sync.Mutex{},
		inFlight:     inFlight,
	}
}

func (ce *TransactionProtocol) Find(transaction string) ([]*Transaction, error) {
	ce.sync.Lock()
	defer ce.sync.Unlock()
	ret, found := ce.transactions[transaction]
	if found {
		cleaned := make([]*Transaction, 0, len(ret))
		for _, t := range ret {
			if t != nil {
				cleaned = append(cleaned, t)
			}
		}
		return cleaned, nil
	}
	return []*Transaction{}, nil
}

func (ce *TransactionProtocol) Add(transaction string, fn func(ce *Transaction, ctx interface{})) (*Transaction, error) {
	ce.sync.Lock()
	defer ce.sync.Unlock()
	out := Transaction{
		transaction: transaction,
		ResolvFn:    fn,
		// ctx:           ctx,
		// myIndex:       i,
		protocol: ce,
	}
	// fmt.Fprintf(os.Stderr, "Transaction:Add:%v\n", transaction)
	for i, t := range ce.transactions[transaction] {
		if t == nil {
			out.myIndex = i
			ce.transactions[transaction][i] = &out
			return &out, nil
		}
	}
	ce.transactions[transaction] = append(ce.transactions[transaction], &out)
	out.myIndex = len(ce.transactions[transaction]) - 1
	return &out, nil
}
