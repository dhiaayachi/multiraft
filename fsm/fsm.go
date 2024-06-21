package fsm

import (
	"github.com/hashicorp/raft"
	"io"
)

type Fsm struct {
	fsm raft.FSM
}

func NewFSM(fsm raft.FSM) *Fsm {
	return &Fsm{fsm: fsm}
}

func (f Fsm) Apply(log *raft.Log) interface{} {
	//TODO implement me
	panic("implement me")
}

func (f Fsm) Snapshot() (raft.FSMSnapshot, error) {
	return f.fsm.Snapshot()
}

func (f Fsm) Restore(snapshot io.ReadCloser) error {
	return f.fsm.Restore(snapshot)
}
