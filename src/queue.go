package main

type Queue interface {
	wait(fn func(a interface{}))
	abort()
	push(a interface{})
}

type ChannelQueue struct {
	channel chan interface{}
	isAbort bool
}

func (cq *ChannelQueue) wait(fn func(a interface{})) {
	for i := range cq.channel {
		if cq.isAbort {
			break
		}
		fn(i)
	}
}

func (cq *ChannelQueue) abort() {
	cq.isAbort = true
	cq.channel <- nil
}

func (cq *ChannelQueue) push(a interface{}) {
	cq.channel <- a
}

func makeChannelQueue(qbufferSize ...int) Queue {
	var channel chan interface{}
	if len(qbufferSize) == 0 {
		channel = make(chan interface{})
	} else {
		channel = make(chan interface{}, qbufferSize[0])
	}
	return &ChannelQueue{
		channel: channel,
		isAbort: false,
	}
}
