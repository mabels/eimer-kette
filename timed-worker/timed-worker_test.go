package timedworker

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/google/uuid"
)

func TestSetupWorker(t *testing.T) {
	worker := NewWorkerProtocol(&WorkerProtocol{
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

	if len(worker.Workers) != worker.Worker {
		t.Errorf("worker count should be 4, but is %d", len(worker.Workers))
	}

	if len(worker.Transactions.transactions) != 0 {
		t.Errorf("transaction count should be 0, but is %d", len(worker.Transactions.transactions))
		for ts, tss := range worker.Transactions.transactions {
			for _, ta := range tss {
				t.Errorf("transaction: %v:%v", ts, *ta)
			}
		}
	}

	err = worker.Shutdown()
	if err != nil {
		t.Error("worker shutdown error: ", err)
	}
	time.Sleep(time.Duration(10 * time.Millisecond))
	for i := 0; i < worker.Worker; i++ {
		if worker.Workers[i].Stop != 2 {
			t.Errorf("worker %v is not stopped:%v", worker.Workers[i].Id, worker.Workers[i].Stop)
		}
	}
	if worker.FromWorkersState != "Stopped" {
		t.Errorf("FromWorkersState is not stopped:%v", worker.FromWorkersState)
	}
}

func TestPingWorkers(t *testing.T) {
	worker := NewWorkerProtocol(&WorkerProtocol{
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
		return
	}
	if len(*result1) != worker.Worker {
		t.Errorf("ping result count should be %d, but is %d", worker.Worker, len(*result1))
	}
	result2, err := worker.Ping(4)
	if err != nil {
		t.Errorf("Ping should not fail: %v", err)
		return
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
		v1, _ := v1str.Data.(int)
		v2, _ := v2str.Data.(int)
		if v1+4 != v2 {
			t.Errorf("ping result not sequential:%v:%v", v1str, v2str)
		}
	}
	if len(worker.Transactions.transactions) != 0 {
		t.Errorf("transaction count should be 0, but is %d", len(worker.Transactions.transactions))
	}
}

func TestDispatchSimple(t *testing.T) {
	worker := NewWorkerProtocol(&WorkerProtocol{
		Worker:     4,
		TimeToStop: time.Duration(2 * time.Second),
	})
	err := worker.Setup()
	if err != nil {
		t.Errorf("worker setup error: %v", err)
	}
	defer worker.Shutdown()
	{
		fn := func(ce *Transaction, cmd WorkerCommand) {
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
		fn := func(ce *Transaction, cmd WorkerCommand) {
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

func TestDispatchBlock(t *testing.T) {
	worker := NewWorkerProtocol(&WorkerProtocol{
		Worker:     4,
		TimeToStop: time.Duration(2 * time.Second),
	})
	err := worker.Setup()
	if err != nil {
		t.Errorf("worker setup error: %v", err)
	}
	defer worker.Shutdown()

	fn := func(ce *Transaction, ctx WorkerCommand) {
		ce.Clean()
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
	if err != nil {
		t.Errorf("Dispatch should fail:%v", err)
	}

	for _, res := range ress {
		ces, err := res.Event.protocol.Find(res.Event.transaction)
		if err != nil {
			t.Errorf("Find should not fail: %v", err)
		}
		if len(ces) != 1 {
			t.Errorf("Find should one: %v", err)
		}
		for _, ce := range ces {
			ce.ResolvFn(ce, WorkerCommand{
				Command:     "test",
				Transaction: res.Event.transaction,
			})
			_, err = worker.Dispatch("test", fn)
			if err != nil {
				t.Errorf("Find should not fail: %v", err)
			}
			ces, err = res.Event.protocol.Find(res.Event.transaction)
			if len(ces) != 0 {
				t.Errorf("Find should return:%v", ces)
			}
			if err != nil {
				t.Errorf("Find should not fail: %v", err)
			}
		}
	}
}

func TestFullTurn(t *testing.T) {
	worker := NewWorkerProtocol(&WorkerProtocol{
		Worker:     4,
		TimeToStop: time.Duration(2 * time.Second),
	})
	err := worker.Setup()
	if err != nil {
		t.Errorf("worker setup error: %v", err)
	}
	defer worker.Shutdown()

	worker.Actions.Register("Add+1", func(twrk *WorkerProtocol, cmd WorkerCommand, wrk *Worker) WorkerCommand {
		return WorkerCommand{
			Command: "Return+1",
			Data:    cmd.Data.(int) + 1,
		}
	})

	worker.Actions.Register("Add+2", func(twrk *WorkerProtocol, cmd WorkerCommand, wrk *Worker) WorkerCommand {
		return WorkerCommand{
			Command: "Return+2",
			Data:    cmd.Data.(int) + 2,
		}
	})

	worker.Actions.Register("Add+3", func(twrk *WorkerProtocol, cmd WorkerCommand, wrk *Worker) WorkerCommand {
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
	_, err = worker.Invoke(InvokeParams{
		Command:  "Add+1",
		InParams: 42,
		ResolvFn: func(_ *Transaction, res WorkerCommand) {
			if res.Data.(int) != 43 {
				t.Errorf("Add+1 should return 43, but is %d", res.Data.(int))
			}
			wait.Unlock()
		},
	})
	if err != nil {
		t.Errorf("worker invoke error: %v", err)
	}
	wait.Lock()
	_, err = worker.Invoke(InvokeParams{
		Command:  "Add+2",
		InParams: 42,
		ResolvFn: func(_ *Transaction, res WorkerCommand) {
			if res.Data.(int) != 44 {
				t.Errorf("Add+2 should return 44, but is %d", res.Data.(int))
			}
			wait.Unlock()
		},
	})
	if err != nil {
		t.Errorf("worker invoke error: %v", err)
	}
	wait.Lock()
	res, err := worker.Invoke(InvokeParams{
		Command:  "Add+3",
		InParams: 42,
		ResolvFn: func(_ *Transaction, res WorkerCommand) {
			if res.Error == nil {
				t.Errorf("Add+3 should return error")
			}
			wait.Unlock()
		},
	})
	if err != nil {
		t.Errorf("worker invoke error: %v", err)
	}
	wait.Lock()
	worker.Dispatch("error", func(ce *Transaction, cmd WorkerCommand) {
	})
	worker.FromWorkers <- WorkerCommand{
		Command:     "Return+3",
		WorkerId:    "test",
		Transaction: res.Event.transaction,
		Data:        49,
		Error:       nil,
	}

}

func workerFn(t *testing.T, worker *WorkerProtocol, ofs int, cur int, done chan int) func(_ *Transaction, res WorkerCommand) {
	return func(ce *Transaction, res WorkerCommand) {
		if res.Error != nil {
			t.Errorf("Add should return error")
		}
		if cur != res.Data.(int)-1 {
			t.Errorf("Add do the right thing")
		}
		if cur < ofs+100 {
			// fmt.Fprintf(os.Stderr, "worker %d: %d", cur, res.Data.(int))
			_, err := worker.Invoke(InvokeParams{
				Command:  "Add",
				InParams: cur + 1,
				ResolvFn: workerFn(t, worker, ofs, cur+1, done),
			})
			if err != nil {
				t.Errorf("Invoke should not fail: %v", err)
			}
		} else {
			done <- cur - ofs
		}
	}
}

func TestError(t *testing.T) {
	worker := NewWorkerProtocol(&WorkerProtocol{
		Worker:     4,
		TimeToStop: time.Duration(2 * time.Second),
	})
	err := worker.Setup()
	if err != nil {
		t.Errorf("worker setup error: %v", err)
	}
	defer worker.Shutdown()
	pre := worker.FromWorkersProcessed
	_, err = worker.Invoke(InvokeParams{
		Command:  "Test1",
		ResolvFn: func(_ *Transaction, res WorkerCommand) {},
	})
	if err != nil {
		t.Errorf("worker invoke error: %v", err)
	}
	_, err = worker.Invoke(InvokeParams{
		Command:  "Test2",
		ResolvFn: func(_ *Transaction, res WorkerCommand) {},
	})
	if err != nil {
		t.Errorf("worker invoke error: %v", err)
	}
	for pre+2 != worker.FromWorkersProcessed {
		time.Sleep(1 * time.Millisecond) // ignore messages
	}
	if len(worker.FreeWorkers) != worker.Worker {
		t.Errorf("All workers should be free")
		return
	}
	cmds := []WorkerCommand{{Command: "Test3", Transaction: "3"}, {Command: "Test4", Transaction: "4"}}
	done := sync.Mutex{}
	done.Lock()
	for i, cmd := range cmds {
		myi := i
		_, err = worker.Invoke(InvokeParams{
			Command:  cmd.Command,
			InParams: cmd.Data,
			ResolvFn: func(_ *Transaction, res WorkerCommand) {
				if res.Error == nil {
					t.Error("Error should not be nil")
				}
				if myi == len(cmds)-1 {
					// fmt.Fprintf(os.Stderr, "Invoke: %v\n", res)
					done.Unlock()
				}
			},
		})
		if err != nil {
			t.Errorf("worker invoke error: %v", err)
		}
	}
	// fmt.Fprintf(os.Stderr, "send error done\n")
	done.Lock()
	// fmt.Fprintf(os.Stderr, "leave Test\n")
}

func TestLoadFull(t *testing.T) {
	worker := NewWorkerProtocol(&WorkerProtocol{
		Worker:     4,
		TimeToStop: time.Duration(2 * time.Second),
	})
	err := worker.Setup()
	if err != nil {
		t.Errorf("worker setup error: %v", err)
	}
	defer worker.Shutdown()

	if worker.Worker != len(worker.FreeWorkers) {
		t.Errorf("Worker should be %d, but is %d", len(worker.FreeWorkers), worker.Worker)
	}

	worker.Actions.Register("Add", func(twrk *WorkerProtocol, cmd WorkerCommand, wrk *Worker) WorkerCommand {
		time.Sleep(1 * time.Millisecond)
		return WorkerCommand{
			Command: "Return",
			Data:    cmd.Data.(int) + 1,
		}
	})
	done := make(chan int)
	for i := 0; i < worker.Worker; i++ {
		_, err := worker.Invoke(InvokeParams{
			Command:  "Add",
			InParams: i * 1000,
			ResolvFn: workerFn(t, worker, i*1000, i*1000, done),
		})
		if err != nil {
			t.Errorf("Invoke should not fail: %v", err)
		}
	}
	total := 0
	for i := 0; i < worker.Worker; i++ {
		total += <-done
	}
	if total != worker.Worker*100 {
		t.Errorf("Add should work:%d", total)
	}
	if len(worker.Workers) != len(worker.FreeWorkers) {
		t.Errorf("All FreeWorkers should idle")
	}
	if len(worker.Transactions.transactions) != 0 {
		t.Errorf("FreeWorkers be fine:%d %v", len(worker.Transactions.transactions), worker.Transactions.transactions)

	}
}

type IndexObject struct {
	Index int
	Data  s3.PutObjectInput
}

func PutObjectResolv(t *testing.T, worker *WorkerProtocol, todos *map[int]IndexObject, todosSync *sync.Mutex, until time.Time, stop chan bool) func(_ *Transaction, res WorkerCommand) {
	myFunc := func(_ *Transaction, res WorkerCommand) {
		if res.Error != nil {
			return
		}
		if time.Now().After(until) {
			stop <- true
			return
		}
		todosSync.Lock()
		defer todosSync.Unlock()
		indexObj := res.Data.(IndexObject)
		delete(*todos, indexObj.Index)
		run := false
		for idx, _ := range *todos {
			run = true
			_, err := worker.Invoke(InvokeParams{
				Command:  "PutObject",
				InParams: (*todos)[idx],
				ResolvFn: PutObjectResolv(t, worker, todos, todosSync, until, stop),
			})
			if err != nil {
				t.Errorf("Invoke should not fail: %v", err)
			}
			break
		}
		if !run {
			stop <- true
		}
	}
	return myFunc
}

func TestTimeLambdaRun(t *testing.T) {
	worker := NewWorkerProtocol(&WorkerProtocol{
		Worker:     4,
		TimeToStop: time.Duration(2 * time.Second),
	})
	err := worker.Setup()
	if err != nil {
		t.Errorf("worker setup error: %v", err)
	}
	defer worker.Shutdown()

	worker.Actions.Register("PutObject", func(twrk *WorkerProtocol, cmd WorkerCommand, wrk *Worker) WorkerCommand {
		time.Sleep(time.Duration(rand.Float32()*10) * time.Millisecond)
		out := cmd
		out.Data = cmd.Data
		return out
	})

	batchSize := 1000
	todos := map[int]IndexObject{}
	todoSync := sync.Mutex{}
	for i := 0; i < batchSize; i++ {
		id := uuid.NewString()
		todos[i] = IndexObject{
			Index: i,
			Data: s3.PutObjectInput{
				Bucket:        aws.String("bucket"),
				Key:           aws.String(fmt.Sprintf("path/%s.test", id)),
				Body:          strings.NewReader(id),
				ContentLength: int64(len(id)),
			},
		}
	}
	loops := 0
	for len(todos) != 0 {
		loops++
		// fmt.Fprintf(os.Stderr, "start loop:%d\n", loops)
		startLen := len(todos)
		started := time.Now()
		until := started.Add(time.Duration(300 * time.Millisecond))
		stop := make(chan bool, worker.Worker)
		invokes := worker.Worker
		if invokes > len(todos) {
			invokes = len(todos)
		}
		for i := 0; i < invokes; i++ {
			_, err := worker.Invoke(InvokeParams{
				Command:  "PutObject",
				InParams: todos[i],
				ResolvFn: PutObjectResolv(t, worker, &todos, &todoSync, until, stop),
			})
			if err != nil {
				t.Errorf("Invoke should not fail: %v", err)
			}
		}
		for i := 0; i < invokes; i++ {
			<-stop
		}
		if time.Now().After(started.Add(350 * time.Millisecond)) {
			t.Errorf("each run should be less:350ms")
		}
		// fmt.Fprintf(os.Stderr, "done loop:%d,%d,%d\n", loops, startLen, len(todos))
		if startLen < len(todos) {
			t.Errorf("don't run:%d", len(todos))
		}
	}
	if loops <= 1 {
		t.Errorf("loops needed:%d", loops)
	}

}
