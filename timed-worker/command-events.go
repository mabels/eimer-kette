package timedworker

import (
	"fmt"
	"sync"
)

type CommandEvent struct {
	cmd           string
	transaction   string
	ResolvFn      func(ctx interface{})
	myIndex       int
	commandEvents *CommandEvents
}

func (result *CommandEvent) Clean() error {
	if result.myIndex < 0 {
		return fmt.Errorf("not active")
	}
	result.commandEvents.sync.Lock()
	result.commandEvents.commands[result.cmd][result.myIndex] = nil
	result.myIndex = -1
	result.commandEvents.sync.Unlock()
	return nil
}

type CommandFnArray struct {
	sync sync.Mutex
	cmds []*CommandEvent
}

type CommandEvents struct {
	commands map[string][]*CommandEvent
	sync     sync.Mutex
	inFlight int
}

func NewCommandEvents(inFlight int) *CommandEvents {
	return &CommandEvents{
		commands: make(map[string][]*CommandEvent),
		sync:     sync.Mutex{},
		inFlight: inFlight,
	}
}

func (ce *CommandEvents) Find(cmd string, transaction string) (*CommandEvent, error) {
	ce.sync.Lock()
	defer ce.sync.Unlock()
	ces, ok := ce.commands[cmd]
	if ok {
		for _, result := range ces {
			if result == nil {
				continue
			}
			if result.transaction == transaction {
				return result, nil
			}
		}
	}
	return nil, nil
}

func (ce *CommandEvents) Add(cmd string, transaction string, fn func(ctx interface{})) (*CommandEvent, error) {
	ce.sync.Lock()
	if _, ok := ce.commands[cmd]; !ok {
		ce.commands[cmd] = make([]*CommandEvent, ce.inFlight)
	}
	var found *CommandEvent
	for i, result := range ce.commands[cmd] {
		if result == nil {
			ce.commands[cmd][i] = &CommandEvent{
				cmd:         cmd,
				transaction: transaction,
				ResolvFn:    fn,
				// ctx:           ctx,
				myIndex:       i,
				commandEvents: ce,
			}
			found = ce.commands[cmd][i]
			break
		}
	}
	ce.sync.Unlock()
	var err error
	if found == nil {
		err = fmt.Errorf("no free slots")
	}
	return found, err
}

// func (cfa *CommandFnArray) Active() []*CommandFn {
// 	cfa.sync.Lock()
// 	ret := make([]*CommandFn, len(cfa.cmds))
// 	copy(ret, cfa.cmds)
// 	cfa.sync.Unlock()
// 	return ret
// }
