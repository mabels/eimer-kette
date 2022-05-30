package timedworker

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
