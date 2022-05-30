package timedworker

import (
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

type WorkerCommand struct {
	Command     string
	WorkerId    string
	Transaction string
	Data        interface{}
	Error       error
}
type Worker struct {
	ToWorker   chan WorkerCommand
	FromWorker chan WorkerCommand
	Id         string
	State      string
	Stop       bool
}

type CommandFn struct {
	Fn  func(ctx interface{}, job interface{})
	Ctx interface{}
}

type Actions map[string]func(twrk *TimedWorker, cmd WorkerCommand, wrk *Worker) WorkerCommand

func (act *Actions) Register(name string, fn func(twrk *TimedWorker, cmd WorkerCommand, wrk *Worker) WorkerCommand) {
	(*act)[name] = fn
}

type TimedWorker struct {
	Worker             int
	TimeToStop         time.Duration
	FromWorkers        chan WorkerCommand
	FromWorkersState   string
	FromWorkersStopped sync.Mutex
	Workers            []*Worker
	FreeWorkers        chan *Worker
	Transactions       *CommandEvents
	WaitingCommands    int
	Actions            Actions
}

func NewTimedWorker(worker *TimedWorker) *TimedWorker {
	return &TimedWorker{
		Worker:      worker.Worker,
		TimeToStop:  worker.TimeToStop,
		FromWorkers: make(chan WorkerCommand, worker.Worker),
		// FromWorkerCommands: make(map[string]CommandFnArray),
		FreeWorkers:     make(chan *Worker, worker.Worker),
		WaitingCommands: 10,
		Transactions:    NewCommandEvents(10),
		Actions:         make(Actions),
	}
}

/*
func (wrk *TimedWorker) AddWaitFromWorker(expectedCmd string, fn func()) {
		done := sync.Mutex{}
		done.Lock()
		waitStarted := sync.Mutex{}
		waitStarted.Lock()
		completedWorker := map[string]WorkerCommand{}
		go func() {
			waitStarted.Unlock()
			var err error
			for err == nil && len(completedWorker) != len(wrk.Workers) {
				command := <-wrk.FromWorkers
				fmt.Fprintf(os.Stderr, "Command:%v:%d\n", command, len(completedWorker))
				if command.Command != expectedCmd {
					err = fmt.Errorf("not expectedCmd:%v -> %v", expectedCmd, command)
					break
				}
				completedWorker[command.WorkerId] = command
			}
			fmt.Fprintf(os.Stderr, "WaitCommandCommpleted:%s:%d\n", expectedCmd, len(completedWorker))
			done.Unlock()
		}()
		waitStarted.Lock()
		return &done, &completedWorker
}
*/

func (wrk *TimedWorker) startFromChannel() error {
	if wrk.FromWorkersState == "Running" {
		return fmt.Errorf("Already running")
	}
	wrk.FromWorkersStopped = sync.Mutex{}
	wrk.FromWorkersStopped.Lock()
	started := sync.Mutex{}
	started.Lock()
	go func() {
		wrk.FromWorkersState = "Running"
		started.Unlock()
		for cmd := range wrk.FromWorkers {
			// fmt.Fprintf(os.Stderr, "FromWorkers:%v\n", cmd)
			if cmd.Command == "stopFromChannel" {
				break
			}
			ce, err := wrk.Transactions.Find(cmd.Command, cmd.Command)
			if err != nil {
				// this is a problem
				continue
			}
			if ce == nil {
				continue
			}
			// fmt.Fprintf(os.Stderr, "ResolvFn:%v\n", cmd)
			ce.ResolvFn(cmd)

			// cmds, ok := wrk.FromWorkerCommands[cmd.Command]
			// if !ok {
			// 	continue
			// }
			// for _, ctx := range cmds.Active() {
			// 	ctx.Fn(ctx.Ctx, cmd)
			// }
		}
		wrk.FromWorkersState = "Stopped"
		wrk.FromWorkersStopped.Unlock()
	}()
	started.Lock()
	return nil
}

func (wrk *TimedWorker) doSimple() (*sync.Mutex, *map[string]WorkerCommand, func(cmd WorkerCommand)) {
	done := sync.Mutex{}
	done.Lock()
	gotStarted := map[string]WorkerCommand{}
	return &done, &gotStarted, func(wc WorkerCommand) {
		// fmt.Fprintf(os.Stderr, "doSimple:%v\n", wc)
		gotStarted[wc.WorkerId] = wc
		if len(gotStarted) == wrk.Worker {
			// fmt.Fprintf(os.Stderr, "doSimple:unlock:%v\n", job)
			done.Unlock()
		}
	}
}

func (wrk *TimedWorker) Setup() error {
	wrk.Workers = make([]*Worker, wrk.Worker)
	var err error
	wrk.Actions.Register("ping", func(twrk *TimedWorker, cmd WorkerCommand, wrk *Worker) WorkerCommand {
		// twrk.FromWorkers <- WorkerCommand{Command: "pong", WorkerId: wrk.Id, Transaction: cmd.Transaction, Data: cmd.Data}
		res := cmd
		res.Command = "pong"
		return res
	})
	wrk.Actions.Register("stop", func(twrk *TimedWorker, cmd WorkerCommand, wrk *Worker) WorkerCommand {
		// fmt.Fprintf(os.Stderr, "stop:%v\n", wrk.Id)
		wrk.Stop = true
		res := cmd
		res.Command = "stopped"
		return res
	})
	wrk.startFromChannel()
	done, _, fn := wrk.doSimple()
	for w := 0; w < wrk.Worker; w++ {
		wrk.Workers[w] = &Worker{
			Id:         uuid.New().String(),
			ToWorker:   make(chan WorkerCommand, 2),
			FromWorker: wrk.FromWorkers,
		}
		wrk.FreeWorkers <- wrk.Workers[w]
		_, err := wrk.Dispatch("started", fn, wrk.Workers[w].Id)
		if err != nil {
			return err
		}
		// Real Worker
		go func(w *Worker) {
			w.FromWorker <- WorkerCommand{Command: "started", Data: w.Id, WorkerId: w.Id, Transaction: w.Id}
			w.Stop = false
			for !w.Stop {
				select {
				case cmd := <-w.ToWorker:
					action, ok := wrk.Actions[cmd.Command]
					if !ok {
						w.FromWorker <- WorkerCommand{Command: "error", Error: fmt.Errorf("Unknown command:%v", cmd), WorkerId: w.Id, Transaction: cmd.Transaction}
						break
					}
					// fmt.Fprintf(os.Stderr, "Action:%v:%v\n", w.Id, cmd.Command)
					res := action(wrk, cmd, w)
					if res.Transaction == "" {
						res.Transaction = cmd.Transaction
					}
					res.WorkerId = w.Id
					w.FromWorker <- res
				}
			}
			// fmt.Fprintf(os.Stderr, "Worker:%v:stopped\n", w.Id)
			// w.FromWorker <- WorkerCommand{Command: "stopped", Data: w.Id, WorkerId: w.Id, Transaction: w.Id}
		}(wrk.Workers[w])
	}
	done.Lock()
	return err
}

func (wrk *TimedWorker) Shutdown() error {
	done, _, fn := wrk.doSimple()
	for _, w := range wrk.Workers {
		_, err := wrk.Dispatch("stopped", fn, w.Id)
		if err != nil {
			return err
		}
		w.ToWorker <- WorkerCommand{Command: "stop", WorkerId: w.Id, Transaction: w.Id}
	}
	done.Lock()
	wrk.FromWorkers <- WorkerCommand{Command: "stopFromChannel"}
	wrk.FromWorkersStopped.Lock()
	wrk.Workers = nil
	return nil
}

func (wrk *TimedWorker) Ping(o_ofs ...int) (*map[string]WorkerCommand, error) {
	var ofs int
	if len(o_ofs) == 0 {
		ofs = 0
	} else {
		ofs = o_ofs[0]
	}
	done := sync.Mutex{}
	done.Lock()
	pongs := map[string]WorkerCommand{}
	for i, w := range wrk.Workers {
		res, err := wrk.Dispatch("pong", func(cmd WorkerCommand) {
			pongs[cmd.WorkerId] = cmd
			if len(pongs) == wrk.Worker {
				done.Unlock()
			}
		})
		if err != nil {
			return nil, err
		}
		w.ToWorker <- WorkerCommand{Command: "ping", Transaction: res.Event.transaction, Data: fmt.Sprintf("%d", ofs+i)}
	}
	done.Lock()
	return &pongs, nil
}

// func (wrk *TimedWorker) AddWaitFromWorker(cmd string, fn func(ctx interface{}, job interface{}), ctx interface{}) error {
// 	wrk.FromWorkerCommandsLock.Lock()
// 	command, ok := wrk.FromWorkerCommands[cmd]
// 	if !ok {
// 		// command = New
// 		// make(chan CommandFn, wrk.Worker)
// 		wrk.FromWorkerCommands[cmd] = command
// 	}
// 	// wrk.FromWorkerCommands[cmd] = append(wrk.FromWorkerCommands[cmd], CommandFn{Fn: fn, Ctx: ctx})
// 	wrk.FromWorkerCommandsLock.Unlock()
// 	return nil
// }

func (wrk *TimedWorker) Start() error {
	res, err := wrk.Ping()
	if err != nil {
		return err
	}
	if len(*res) != wrk.Worker {
		return fmt.Errorf("not all workers available")
	}
	// wrk.FreeMutex = sync.Mutex{}
	// wrk.FreeRing = 0
	// wrk.FreeWorkers = make([]*Worker, wrk.Worker)
	// for i, w := range wrk.Workers {
	// 	wrk.FreeWorkers[i] = w
	// }

	return nil
}

type CustomCommand struct {
	Command string
	Job     interface{}
}

type Result struct {
	Event  *CommandEvent
	Worker *Worker
	// Waiting chan WorkerCommand
}

// func (result *Result) Wait() (WorkerCommand, error) {
// 	wc := <-result.Waiting
// 	return wc, nil
// }

func (wrk *TimedWorker) Invoke(icmd string, params interface{}, rcmd string, fn func(cmd WorkerCommand), optional_transaction ...string) (*Result, error) {
	var transaction string
	if len(optional_transaction) > 0 {
		transaction = optional_transaction[0]
	} else {
		transaction = uuid.NewString()
	}
	res, err := wrk.Dispatch(rcmd, fn, transaction)
	if err != nil {
		return nil, err
	}
	res.Worker.ToWorker <- WorkerCommand{Command: icmd, Data: params, Transaction: res.Event.transaction}
	return res, nil
}

func (wrk *TimedWorker) Dispatch(cmd string, runOnReceive func(cmd WorkerCommand), optional_transaction ...string) (*Result, error) {
	var freeWorker *Worker
	select {
	case x := <-wrk.FreeWorkers:
		freeWorker = x
	default:
		return nil, fmt.Errorf("no free workers")
	}

	var transaction string
	if len(optional_transaction) > 0 {
		transaction = optional_transaction[0]
	} else {
		transaction = uuid.NewString()
	}
	res := Result{
		Worker: freeWorker,
		// Waiting: make(chan WorkerCommand, wrk.WaitingCommands),
	}
	ce, err := wrk.Transactions.Add(cmd, transaction, func(receivedCmd interface{}) {
		// fmt.Fprintf(os.Stderr, "received %s\n", receivedCmd)
		// if runOnReceive != nil {
		runOnReceive(receivedCmd.(WorkerCommand))
		// } else {
		// 	res.Waiting <- receivedCmd.(WorkerCommand)
		// }
		wrk.FreeWorkers <- freeWorker
	})
	if err != nil {
		return nil, err
	}
	res.Event = ce
	// fmt.Fprintf(os.Stderr, "Dispatch:%s:%s\n", cmd, transaction)
	return &res, nil
	// var runnable *Worker
	// wrk.FreeMutex.Lock()
	// i := wrk.FreeRing
	// for c := 0; c < wrk.Worker; c++ {
	// 	runnable = wrk.FreeWorkers[i]
	// 	if runnable != nil {
	// 		wrk.FreeWorkers[i] = nil
	// 		break
	// 	}
	// 	i = (i + 1) % wrk.Worker
	// }
	// wrk.FreeRing = (wrk.FreeRing + 1) % wrk.Worker
	// wrk.FreeMutex.Unlock()
	// runnable.ToWorker <- WorkerCommand{Command: "StartCmd", Data: CustomCommand{
	// 	Command: cmd,
	// 	Job:     job,
	// }, WorkerId: runnable.Id}
	// return nil
}
