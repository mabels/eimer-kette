package main

type Queue interface {
	notifyWaitAdded(fn func(q Queue))
	notifyWaitDone(fn func(q Queue))
	wait(fn func(a interface{}))
	stop()
	push(a interface{})
}

type ChannelQueue struct {
	// q     chan interface{}
	createWaitQ            chan (chan interface{})
	completeWaitQ          chan (chan interface{})
	waitQ                  chan (chan interface{})
	notificationsWaitAdded []func(q Queue)
	notificationsWaitDone  []func(q Queue)
	// isAbort bool
}

func (cq *ChannelQueue) notifyWaitAdded(fn func(q Queue)) {
	cq.notificationsWaitAdded = append(cq.notificationsWaitAdded, fn)
}
func (cq *ChannelQueue) notifyWaitDone(fn func(q Queue)) {
	cq.notificationsWaitDone = append(cq.notificationsWaitDone, fn)
}

func (cq *ChannelQueue) wait(fn func(a interface{})) {
	waitQ := make(chan interface{})
	cq.createWaitQ <- waitQ
	for i := range waitQ {
		if i == nil {
			break
		}
		fn(i)
	}
	cq.completeWaitQ <- waitQ
}

func (cq *ChannelQueue) stop() {
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

func (cq *ChannelQueue) push(item interface{}) {
	next := <-cq.waitQ
	next <- item
	cq.waitQ <- next
}

func makeChannelQueue(qbufferSize ...int) Queue {
	if len(qbufferSize) == 0 {
		qbufferSize = []int{1}
	}
	ret := &ChannelQueue{
		createWaitQ:   make(chan chan interface{}, qbufferSize[0]),
		completeWaitQ: make(chan chan interface{}, qbufferSize[0]),
		waitQ:         make(chan chan interface{}, qbufferSize[0]),
	}
	go func() {
		for ch := range ret.createWaitQ {
			for _, n := range ret.notificationsWaitAdded {
				n(ret)
			}
			ret.waitQ <- ch
		}
	}()
	go func() {
		for ignore := range ret.completeWaitQ {
			_ = ignore
			for _, n := range ret.notificationsWaitDone {
				n(ret)
			}
		}
	}()
	return ret
}
