package timedworker

type Actions map[string]func(twrk *WorkerProtocol, cmd WorkerCommand, wrk *Worker) WorkerCommand

func (act *Actions) Register(name string, fn func(twrk *WorkerProtocol, cmd WorkerCommand, wrk *Worker) WorkerCommand) {
	(*act)[name] = fn
}

func (act *Actions) Find(name string) func(twrk *WorkerProtocol, cmd WorkerCommand, wrk *Worker) WorkerCommand {
	fn, found := (*act)[name]
	if found {
		return fn
	}
	return nil
}
