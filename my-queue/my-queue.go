package myqueue

type MyQueue interface {
	NotifyWaitAdded(fn func(q MyQueue))
	NotifyWaitDone(fn func(q MyQueue))
	Wait(fn func(a interface{}))
	Stop()
	Push(a interface{})
}

type ChannelQueue struct {
	// q     chan interface{}
	createWaitQ            chan (chan interface{})
	completeWaitQ          chan (chan interface{})
	waitQ                  chan (chan interface{})
	notificationsWaitAdded []func(q MyQueue)
	notificationsWaitDone  []func(q MyQueue)
	// isAbort bool
}

func (cq *ChannelQueue) NotifyWaitAdded(fn func(q MyQueue)) {
	cq.notificationsWaitAdded = append(cq.notificationsWaitAdded, fn)
}
func (cq *ChannelQueue) NotifyWaitDone(fn func(q MyQueue)) {
	cq.notificationsWaitDone = append(cq.notificationsWaitDone, fn)
}

func (cq *ChannelQueue) Wait(fn func(a interface{})) {
	// one item per waiter + on stop item for in wait calls
	waitQ := make(chan interface{}, 1+1)
	cq.createWaitQ <- waitQ
	for i := range waitQ {
		if i == nil {
			break
		}
		fn(i)
	}
	cq.completeWaitQ <- waitQ
}

func (cq *ChannelQueue) Stop() {
	for {
		toBreak := false
		select {
		case ch := <-cq.waitQ:
			ch <- nil
		default:
			toBreak = true
		}
		if toBreak {
			break
		}
	}
}

func (cq *ChannelQueue) Push(item interface{}) {
	next := <-cq.waitQ
	next <- item
	cq.waitQ <- next
}

func MakeChannelQueue(qbufferSize ...int) MyQueue {
	if len(qbufferSize) == 0 {
		qbufferSize = []int{1}
	}
	ret := &ChannelQueue{
		createWaitQ:   make(chan chan interface{}, qbufferSize[0]),
		completeWaitQ: make(chan chan interface{}, qbufferSize[0]),
		// +1 to work with in wait - stops
		waitQ: make(chan chan interface{}, qbufferSize[0]+1),
	}
	startCompleted := make(chan bool)
	go func() {
		startCompleted <- true
		for ch := range ret.createWaitQ {
			for _, n := range ret.notificationsWaitAdded {
				n(ret)
			}
			ret.waitQ <- ch
		}
	}()
	go func() {
		startCompleted <- true
		for ignore := range ret.completeWaitQ {
			_ = ignore
			for _, n := range ret.notificationsWaitDone {
				n(ret)
			}
		}
	}()
	<-startCompleted
	<-startCompleted
	return ret
}
