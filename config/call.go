package config

import (
	"sync"
	"time"
)

type CallStat struct {
	Cnt      int64
	Duration int64
}

type Calls struct {
	calls map[string]*CallStat
	mutex sync.Mutex
}

func MakeCalls() *Calls {
	return &Calls{
		calls: make(map[string]*CallStat),
		mutex: sync.Mutex{},
	}
}

func (calls *Calls) Duration(key string, start time.Time) {
	since := time.Since(start)
	calls.mutex.Lock()
	ret, ok := calls.calls[key]
	if !ok {
		ret = &CallStat{}
		calls.calls[key] = ret
	}
	ret.Cnt = ret.Cnt + 1
	ret.Duration = ret.Duration + since.Nanoseconds()
	calls.mutex.Unlock()

}

func (calls *Calls) Get(key string) CallStat {
	val, ok := calls.calls[key]
	if !ok {
		val = &CallStat{}
	}
	return *val
}

func (calls *Calls) Keys(keys ...string) []string {
	keys2 := make([]string, 0, len(calls.calls))
	for k := range calls.calls {
		keys2 = append(keys2, k)
	}
	return keys2
}

func (calls *Calls) Inc(keys ...string) {
	calls.Add(1, keys...)
}

func (calls *Calls) Dec(keys ...string) {
	calls.Add(-1, keys...)
}

func (calls *Calls) Add(add int, keys ...string) {
	calls.mutex.Lock()
	for _, key := range keys {
		ret, ok := calls.calls[key]
		if !ok {
			ret = &CallStat{}
			calls.calls[key] = ret
		}
		ret.Cnt = ret.Cnt + int64(add)
	}
	calls.mutex.Unlock()
}
