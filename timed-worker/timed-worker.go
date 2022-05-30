package timedworker

import (
	"fmt"
	"os"
	"reflect"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/opentracing/opentracing-go/log"
)

type Result struct {
	Event  *Transaction
	Worker *Worker
}
type WorkerProtocol struct {
	Worker               int
	TimeToStop           time.Duration
	FromWorkers          chan WorkerCommand
	FromWorkersProcessed int
	FromWorkersState     string
	FromWorkersStopped   sync.Mutex
	Workers              []*Worker
	FreeWorkers          chan *Worker
	Transactions         *TransactionProtocol
	WaitingCommands      int
	Actions              Actions
}

func NewWorkerProtocol(worker *WorkerProtocol) *WorkerProtocol {
	return &WorkerProtocol{
		Worker:      worker.Worker,
		TimeToStop:  worker.TimeToStop,
		FromWorkers: make(chan WorkerCommand, worker.Worker),
		// FromWorkerCommands: make(map[string]CommandFnArray),
		FreeWorkers:     make(chan *Worker, worker.Worker),
		WaitingCommands: 10,
		Transactions:    NewTransactionProtocol(10),
		Actions:         make(Actions),
	}
}

func (wrk *WorkerProtocol) sendError(cmd *WorkerCommand, cmdErr error) {
	// fmt.Fprintf(os.Stderr, "-1-sendError:%v\n", cmd)
	cerrs, err := wrk.Transactions.Find("SystemError")
	// fmt.Fprintf(os.Stderr, "-2-sendError:%v\n", cmd)
	if err != nil {
		log.Error(fmt.Errorf("transaction find error:%v", err))
		// fmt.Fprintf(os.Stderr, "-2.1-sendError:%v\n", cmd)
		return
	}
	for _, cerr := range cerrs {
		// fmt.Fprintf(os.Stderr, "-3-sendError:%v\n", cmd)
		fmt.Fprintf(os.Stderr, "-3.1-sendError:%v:%v:%v\n", cmd, reflect.ValueOf(cerr.ResolvFn), cerr)
		cerr.ResolvFn(cerr, WorkerCommand{
			Command:     "error",
			WorkerId:    cmd.WorkerId,
			Transaction: cmd.Transaction,
			Data:        cmd,
			Error:       cmdErr,
		})
		fmt.Fprintf(os.Stderr, "-3.2-sendError:%v:%v:%v\n", cmd, reflect.ValueOf(cerr.ResolvFn), cerr)
	}
}

func (wrk *WorkerProtocol) startFromChannel() error {
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
			wrk.FromWorkersProcessed++
			// fmt.Fprintf(os.Stderr, "FromWorkers:%v\n", cmd)
			if cmd.Command == "stopFromChannel" {
				break
			}
			ces, err := wrk.Transactions.Find(cmd.Transaction)
			for _, ce := range ces {
				if err != nil {
					fmt.Fprintf(os.Stderr, "Pre-FromWorkers:Err:%v:%v\n", cmd, err)
					wrk.sendError(&cmd, err)
					fmt.Fprintf(os.Stderr, "Pos-FromWorkers:Err:%v:%v\n", cmd, err)
					continue
				}
				if ce == nil {
					fmt.Fprintf(os.Stderr, "Pre-FromWorkers:Ce:%v:%v\n", cmd, err)
					wrk.sendError(&cmd, fmt.Errorf("Unknown Transaction:%v", cmd.Transaction))
					fmt.Fprintf(os.Stderr, "Pos-FromWorkers:Ce:%v:%v\n", cmd, err)
					continue
				}

				ce.ResolvFn(ce, cmd)
			}
		}
		wrk.FromWorkersState = "Stopped"
		wrk.FromWorkersStopped.Unlock()
	}()
	started.Lock()
	return nil
}

func (wrk *WorkerProtocol) collectFromAllWorker() (*sync.Mutex, *map[string]WorkerCommand, func(ce *Transaction, cmd WorkerCommand)) {
	done := sync.Mutex{}
	done.Lock()
	gotStarted := map[string]WorkerCommand{}
	return &done, &gotStarted, func(ce *Transaction, wc WorkerCommand) {
		// fmt.Fprintf(os.Stderr, "doSimple:%v\n", wc)
		gotStarted[wc.WorkerId] = wc
		if len(gotStarted) == wrk.Worker {
			// fmt.Fprintf(os.Stderr, "doSimple:unlock:%v\n", wc)
			done.Unlock()
		}
	}
}

func (wrk *WorkerProtocol) Setup() error {
	wrk.Workers = make([]*Worker, wrk.Worker)
	var err error
	wrk.Actions.Register("ping", func(twrk *WorkerProtocol, cmd WorkerCommand, wrk *Worker) WorkerCommand {
		// twrk.FromWorkers <- WorkerCommand{Command: "pong", WorkerId: wrk.Id, Transaction: cmd.Transaction, Data: cmd.Data}
		res := cmd
		res.Command = "pong"
		return res
	})
	wrk.Actions.Register("stop", func(twrk *WorkerProtocol, cmd WorkerCommand, wrk *Worker) WorkerCommand {
		// fmt.Fprintf(os.Stderr, "stop:%v\n", wrk.Id)
		wrk.Stop = true
		res := cmd
		res.Command = "stopped"
		return res
	})
	wrk.startFromChannel()
	// fmt.Fprintf(os.Stderr, "startFromChannel\n")
	done, _, fn := wrk.collectFromAllWorker()
	for w := 0; w < wrk.Worker; w++ {
		wrk.Workers[w] = &Worker{
			Id:         uuid.New().String(),
			ToWorker:   make(chan WorkerCommand, 2),
			FromWorker: wrk.FromWorkers,
		}
		wrk.FreeWorkers <- wrk.Workers[w]
		rs, err := wrk.Dispatch("started", func(ce *Transaction, cmd WorkerCommand) {
			ce.Clean()
			fn(ce, cmd)

		})
		// fmt.Fprintf(os.Stderr, "Dispatch:started:%v\n", rs.Event.transaction)
		if err != nil {
			return err
		}
		// Real Worker
		go func(w *Worker) {
			w.FromWorker <- WorkerCommand{Command: "started", Data: w.Id, WorkerId: w.Id, Transaction: rs.Event.transaction}
			w.Stop = false
			for !w.Stop {
				select {
				case cmd := <-w.ToWorker:
					action, ok := wrk.Actions[cmd.Command]
					if !ok {
						w.FromWorker <- WorkerCommand{
							Command:     "error",
							Error:       fmt.Errorf("Unknown command:%v", cmd),
							WorkerId:    w.Id,
							Data:        cmd,
							Transaction: cmd.Transaction,
						}
					} else {
						// fmt.Fprintf(os.Stderr, "Action:%v:%v\n", w.Id, cmd.Command)
						res := action(wrk, cmd, w)
						if res.Command == "" {
							continue
						}
						if res.Transaction == "" {
							res.Transaction = cmd.Transaction
						}
						res.WorkerId = w.Id
						w.FromWorker <- res
					}
					wrk.FreeWorkers <- w
				}
			}
			// fmt.Fprintf(os.Stderr, "Worker:%v:stopped\n", w.Id)
			// w.FromWorker <- WorkerCommand{Command: "stopped", Data: w.Id, WorkerId: w.Id, Transaction: w.Id}
		}(wrk.Workers[w])
	}
	done.Lock()
	return err
}

func (wrk *WorkerProtocol) Shutdown() error {
	done, _, fn := wrk.collectFromAllWorker()
	for _, w := range wrk.Workers {
		rs, err := wrk.Dispatch("stopped", fn)
		if err != nil {
			return err
		}
		w.ToWorker <- WorkerCommand{Command: "stop", WorkerId: w.Id, Transaction: rs.Event.transaction}
	}
	done.Lock()
	wrk.FromWorkers <- WorkerCommand{Command: "stopFromChannel"}
	wrk.FromWorkersStopped.Lock()
	wrk.Workers = nil
	return nil
}

func (wrk *WorkerProtocol) Ping(o_ofs ...int) (*map[string]WorkerCommand, error) {
	var ofs int
	if len(o_ofs) == 0 {
		ofs = 0
	} else {
		ofs = o_ofs[0]
	}
	done := sync.Mutex{}
	done.Lock()
	pongs := map[string]WorkerCommand{}
	for _, w := range wrk.Workers {
		// fmt.Fprintf(os.Stderr, "Ping:Invoke:%v\n", w.Id)
		_, err := wrk.Invoke(InvokeParams{
			Command:  "ping",
			InParams: ofs,
			ResolvFn: func(ce *Transaction, cmd WorkerCommand) {
				// fmt.Fprintf(os.Stderr, "ResolvFn:%v\n", cmd)
				pongs[cmd.WorkerId] = cmd
				if len(pongs) == wrk.Worker {
					done.Unlock()
				}
			},
			Worker: w,
		})
		if err != nil {
			return nil, err
		}
	}
	done.Lock()
	return &pongs, nil
}

func (wrk *WorkerProtocol) Start() error {
	res, err := wrk.Ping()
	if err != nil {
		return err
	}
	if len(*res) != wrk.Worker {
		return fmt.Errorf("not all workers available")
	}
	return nil
}

type InvokeParams struct {
	Transaction *string
	Command     string
	InParams    interface{}
	ResolvFn    func(ce *Transaction, cmd WorkerCommand)
	Worker      *Worker
}

func (wrk *WorkerProtocol) Invoke(ip InvokeParams) (*Result, error) {
	if ip.Transaction == nil {
		my := uuid.New().String()
		ip.Transaction = &my
	}
	// fmt.Fprintf(os.Stderr, "-1-Invoke:ToWorker:%v:%v:%v\n", icmd, params, transaction)
	res, err := wrk.Dispatch(*ip.Transaction, func(ce *Transaction, cmd WorkerCommand) {
		err := ce.Clean()
		// fmt.Fprintf(os.Stderr, "Invoke:Dispatch:%v:%v:%v\n", cmd.Command, cmd.Data, err)
		if err != nil {
			out := cmd
			out.Command = "error"
			out.Data = cmd
			out.Error = err
			ip.ResolvFn(ce, out)
		} else {
			ip.ResolvFn(ce, cmd)
		}
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "-2.1-Invoke:ToWorker:%v\n", err)
		return nil, err
	}
	if ip.Worker == nil {
		select {
		case x := <-wrk.FreeWorkers:
			res.Worker = x
		default:
			return nil, fmt.Errorf("no free workers")
		}
	} else {
		found := false
		for !found {
			out := make([]*Worker, 0, len(wrk.Workers))
			abort := false
			for i := 0; !abort && i < len(wrk.Workers); i++ {
				select {
				case x := <-wrk.FreeWorkers:
					if ip.Worker != x {
						out = append(out, x)
					} else {
						found = true
						res.Worker = x
					}
				default:
					abort = true
					break
				}
			}
			for _, w := range out {
				wrk.FreeWorkers <- w
			}
			if !found {
				// really ugly busy waiting
				time.Sleep(10 * time.Millisecond)
			}
		}

	}
	// fmt.Fprintf(os.Stderr, "-3-Invoke:ToWorker:%v\n", ip)
	res.Worker.ToWorker <- WorkerCommand{Command: ip.Command, Data: ip.InParams,
		Transaction: res.Event.transaction}
	// fmt.Fprintf(os.Stderr, "-4-Invoke:ToWorker:%v\n", ip)
	return res, nil
}

func (wrk *WorkerProtocol) Dispatch(cmd string, runOnReceive func(ce *Transaction, cmd WorkerCommand), optional_transaction ...string) (*Result, error) {

	var transaction string
	if len(optional_transaction) > 0 {
		transaction = optional_transaction[0]
	} else {
		transaction = uuid.NewString()
	}
	// res := Result{
	// 	Worker: freeWorker,
	// }
	ce, err := wrk.Transactions.Add(transaction, func(ce *Transaction, ctx interface{}) {
		// fmt.Fprintf(os.Stderr, "-1-Transaction-Dispatch:FN:%v\n", ctx)
		// fmt.Fprintf(os.Stderr, "-2-Transaction-Dispatch:FN:%v\n", ctx)
		runOnReceive(ce, ctx.(WorkerCommand))
		// fmt.Fprintf(os.Stderr, "-3-Transaction-Dispatch:FN:%v\n", ctx)
	})
	if err != nil {
		return nil, err
	}
	// res.Event = ce
	// fmt.Fprintf(os.Stderr, "Dispatch:%s:%s\n", cmd, transaction)
	return &Result{
		Event: ce,
	}, nil
}
