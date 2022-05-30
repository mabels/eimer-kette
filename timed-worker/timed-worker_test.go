package timedworker

import (
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestSetupWorker(t *testing.T) {
	worker := NewTimedWorker(&TimedWorker{
		Worker:     4,
		TimeToStop: time.Duration(2 * time.Second),
	})
	err := worker.Setup()
	if err != nil {
		t.Errorf("worker setup error: %v", err)
	}
	if worker.FromWorkersState != "Running" {
		t.Errorf("FromWorkersState is not running:%v", worker.FromWorkersState)
	}
	if len(worker.Workers) != 4 {
		t.Errorf("worker count should be 4, but is %d", len(worker.Workers))
	}
	for w := 0; w < len(worker.Workers); w++ {
		if worker.Workers[w].Id == "" {
			t.Errorf("worker %d has no id", w)
		}
	}
	err = worker.Shutdown()
	if err != nil {
		t.Error("worker shutdown error: ", err)
	}
	if worker.FromWorkersState != "Stopped" {
		t.Errorf("FromWorkersState is not stopped:%v", worker.FromWorkersState)
	}
}

func TestPingWorkers(t *testing.T) {
	worker := NewTimedWorker(&TimedWorker{
		Worker:     4,
		TimeToStop: time.Duration(2 * time.Second),
	})
	err := worker.Setup()
	if err != nil {
		t.Errorf("worker setup error: %v", err)
	}
	defer worker.Shutdown()

	result1, err := worker.Ping()
	if err != nil {
		t.Errorf("Ping should not fail: %v", err)
	}
	if len(*result1) != worker.Worker {
		t.Errorf("ping result count should be %d, but is %d", worker.Worker, len(*result1))
	}
	result2, err := worker.Ping(4)
	if err != nil {
		t.Errorf("Ping should not fail: %v", err)
	}
	if len(*result2) != worker.Worker {
		t.Errorf("ping result count should be %d, but is %d", worker.Worker, len(*result2))
	}
	for k1, v1str := range *result1 {
		v2str, ok := (*result2)[k1]
		if !ok {
			t.Errorf("ping result2 %s not found", k1)
		}
		// fmt.Fprintf(os.Stderr, "v1str:%v, v2str:%v\n", v1str, v2str)
		v1, _ := strconv.Atoi(v1str.Data.(string))
		v2, _ := strconv.Atoi(v2str.Data.(string))
		if v1+4 != v2 {
			t.Errorf("ping result not sequential:%v:%v", v1str, v2str)
		}
	}
}

func TestDispatchSimple(t *testing.T) {
	worker := NewTimedWorker(&TimedWorker{
		Worker:     4,
		TimeToStop: time.Duration(2 * time.Second),
	})
	err := worker.Setup()
	if err != nil {
		t.Errorf("worker setup error: %v", err)
	}
	defer worker.Shutdown()
	{
		fn := func(cmd WorkerCommand) {
		}
		res, err := worker.Dispatch("test", fn, "TestDispatch")
		if err != nil {
			t.Errorf("Dispatch should not fail: %v", err)
		}
		if res == nil {
			t.Errorf("Dispatch should return a result")
		}
		if res.Event.transaction != "TestDispatch" {
			t.Errorf("Dispatch should return a result with transaction TestDispatch")
		}
	}
	{
		fn := func(cmd WorkerCommand) {
		}
		res, err := worker.Dispatch("test", fn)
		if err != nil {
			t.Errorf("Dispatch should not fail: %v", err)
		}
		if res == nil {
			t.Errorf("Dispatch should return a result")
		}
		if res.Event.transaction == "" {
			t.Errorf("Dispatch should return a result with transaction TestDispatch")
		}
	}
}

type Job struct {
	Id string
}

func TestDispatchBlock(t *testing.T) {
	worker := NewTimedWorker(&TimedWorker{
		Worker:     4,
		TimeToStop: time.Duration(2 * time.Second),
	})
	err := worker.Setup()
	if err != nil {
		t.Errorf("worker setup error: %v", err)
	}
	defer worker.Shutdown()

	fn := func(ctx WorkerCommand) {
		// res := ctx.(*Result)
		// res.Event.Clean()
	}
	ress := []*Result{}
	for i := 0; i < worker.Worker; i++ {
		res, err := worker.Dispatch("test", fn)
		if err != nil {
			t.Errorf("Dispatch should not fail: %v", err)
		}
		ress = append(ress, res)
	}
	_, err = worker.Dispatch("test", fn)
	if err == nil {
		t.Errorf("Dispatch should fail")
	}

	for _, res := range ress {
		ce, err := res.Event.commandEvents.Find("test", res.Event.transaction)
		if err != nil {
			t.Errorf("Find should not fail: %v", err)
		}
		ce.ResolvFn(res)
		_, err = worker.Dispatch("test", fn)
		if err != nil {
			t.Errorf("Find should not fail: %v", err)
		}
		ce, err = res.Event.commandEvents.Find("test", res.Event.transaction)
		if err != nil {
			t.Errorf("Find should not fail: %v", err)
		}
		if ce != nil {
			t.Errorf("Find should not find anything")
		}
	}
}

func TestFullTurn(t *testing.T) {
	worker := NewTimedWorker(&TimedWorker{
		Worker:     4,
		TimeToStop: time.Duration(2 * time.Second),
	})
	err := worker.Setup()
	if err != nil {
		t.Errorf("worker setup error: %v", err)
	}
	defer worker.Shutdown()

	worker.Actions.Register("Add+1", func(twrk *TimedWorker, cmd WorkerCommand, wrk *Worker) WorkerCommand {
		return WorkerCommand{
			Command: "Return+1",
			Data:    cmd.Data.(int) + 1,
		}
	})

	worker.Actions.Register("Add+2", func(twrk *TimedWorker, cmd WorkerCommand, wrk *Worker) WorkerCommand {
		return WorkerCommand{
			Command: "Return+2",
			Data:    cmd.Data.(int) + 2,
		}
	})

	worker.Actions.Register("Add+3", func(twrk *TimedWorker, cmd WorkerCommand, wrk *Worker) WorkerCommand {
		return WorkerCommand{
			Command: "Return+3",
			Error:   fmt.Errorf("error"),
		}
	})

	err = worker.Start()
	if err != nil {
		t.Errorf("worker start error: %v", err)
	}
	wait := sync.Mutex{}
	wait.Lock()
	worker.Invoke("Add+1", 42, "Return+1", func(res WorkerCommand) {
		if res.Data.(int) != 43 {
			t.Errorf("Add+1 should return 43, but is %d", res.Data.(int))
		}
		wait.Unlock()
	})
	wait.Lock()
	worker.Invoke("Add+2", 42, "Return+2", func(res WorkerCommand) {
		if res.Data.(int) != 44 {
			t.Errorf("Add+2 should return 44, but is %d", res.Data.(int))
		}
		wait.Unlock()
	})

	wait.Lock()
	worker.Invoke("Add+3", 42, "Return+3", func(res WorkerCommand) {
		if res.Error == nil {
			t.Errorf("Add+3 should return error")
		}
		wait.Unlock()
	})
}
