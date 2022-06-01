package myqueue

import (
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestChannelQueueNoPush(t *testing.T) {
	q := MakeChannelQueue()
	finisched := make(chan bool)
	waitExpected := []string{}
	expectWait := make(chan bool, 1)
	q.NotifyWaitAdded(func(q MyQueue) {
		expectWait <- true
	})
	go func() {
		q.Wait(func(a interface{}) {
			if len(waitExpected) == 0 {
				t.Error("should not happend")
				return
			}
			if waitExpected[0] != a {
				t.Error("Should be equal")
			}
			waitExpected = waitExpected[1:]
		})
		finisched <- true
	}()
	<-expectWait
	// q.push("Hello")
	q.Stop()
	<-finisched
	if len(waitExpected) != 0 {
		t.Error("expected should be empty")
	}
}

func TestChannelQueueOnePush(t *testing.T) {
	q := MakeChannelQueue()
	finisched := make(chan bool)
	waitExpected := []string{"Hello"}
	expectWait := make(chan bool, 1)
	q.NotifyWaitAdded(func(q MyQueue) {
		expectWait <- true
	})
	go func() {
		time.Sleep(100 * time.Millisecond)
		q.Wait(func(a interface{}) {
			if len(waitExpected) == 0 {
				t.Error("should not happend")
				return
			}
			if waitExpected[0] != a {
				t.Error("Should be equal")
			}
			waitExpected = waitExpected[1:]
		})
		finisched <- true
	}()
	q.Push("Hello")
	<-expectWait
	q.Stop()
	<-finisched
	if len(waitExpected) != 0 {
		t.Error("expected should be empty")
	}
}

func TestChannelQueueFivePush(t *testing.T) {
	// t.Error("Was")
	q := MakeChannelQueue()
	finisched := make(chan bool)
	waitExpected := []string{"Hello0", "Hello1", "Hello2", "Hello3", "Hello4"}
	go func() {
		time.Sleep(100 * time.Millisecond)
		q.Wait(func(a interface{}) {
			if len(waitExpected) == 0 {
				t.Error("should not happend")
				return
			}
			if waitExpected[0] != a {
				t.Error("Should be equal", waitExpected[0], a)
			}
			waitExpected = waitExpected[1:]
		})
		finisched <- true
	}()
	for i := 0; i < 5; i += 1 {
		q.Push(fmt.Sprintf("Hello%d", i))
	}
	q.Stop()
	<-finisched
	if len(waitExpected) != 0 {
		t.Error("expected should be empty")
	}
}

func TestChannelQueueMultipleFivePush(t *testing.T) {
	tasks := 5
	pushes := 1000
	q := MakeChannelQueue(tasks)
	finisched := make(chan bool)
	waitExpected := int64(0)
	for ts := 0; ts < tasks; ts += 1 {
		go func() {
			time.Sleep(100 * time.Millisecond)
			q.Wait(func(a interface{}) {
				atomic.AddInt64(&waitExpected, 1)
				if !strings.HasPrefix(a.(string), "Hello") {
					t.Error("Should be equal", a)
				}
			})
			finisched <- true
		}()
	}
	started := make(chan bool)
	releaseNotifyAdd := tasks
	q.NotifyWaitAdded(func(q MyQueue) {
		releaseNotifyAdd -= 1
		if releaseNotifyAdd <= 0 {
			started <- true
		}
	})
	<-started
	for i := 0; i < pushes; i += 1 {
		q.Push(fmt.Sprintf("Hello%d", i))
	}
	done := make(chan bool)
	releaseNotifyDone := tasks
	q.NotifyWaitDone(func(q MyQueue) {
		releaseNotifyDone -= 1
		if releaseNotifyDone <= 0 {
			done <- true
		}
	})
	q.Stop()
	<-done
	for ts := 0; ts < tasks; ts += 1 {
		<-finisched
	}
	if int(waitExpected) != pushes {
		t.Error("expected should be empty", waitExpected, pushes)
	}
}

func TestChannelQueueRecursiveStop(t *testing.T) {
	q := MakeChannelQueue()
	gotStop := make(chan bool)
	go func() {
		q.Wait(func(a interface{}) {
			fmt.Println("got a", a)
			q.Stop()
			fmt.Println("post stop")
		})
		gotStop <- true
	}()
	q.Push(5)
	<-gotStop

}
