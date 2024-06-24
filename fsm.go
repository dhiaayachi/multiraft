package multiraft

import (
	"github.com/dhiaayachi/multiraft/encoding"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"io"
)

type Fsm struct {
	fsm       raft.FSM
	logger    hclog.Logger
	id        raft.ServerID
	multiRaft *MultiRaft
}

func NewFSM(fsm raft.FSM, r *MultiRaft) *Fsm {
	return &Fsm{fsm: fsm, multiRaft: r}
}

func (f *Fsm) Apply(log *raft.Log) interface{} {
	conf := &PartitionConfiguration{}
	err := encoding.DecodeMsgPack(log.Data, conf)
	if err != nil {
		f.logger.Error("failed to decode raft partition configuration", "error", err)
	}
	if f.inServers(conf.Servers) {
		err := f.multiRaft.addRaft(conf.PartitionID)
		if err != nil {
			f.logger.Error("failed to add raft server", "error", err)
		}
	}
	return nil
}

func (f *Fsm) inServers(servers []raft.Server) bool {
	for _, server := range servers {
		if f.id == server.ID {
			return true
		}
	}
	return false
}

func (f *Fsm) Snapshot() (raft.FSMSnapshot, error) {
	return f.fsm.Snapshot()
}

func (f *Fsm) Restore(snapshot io.ReadCloser) error {
	return f.fsm.Restore(snapshot)
}
