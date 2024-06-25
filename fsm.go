package multiraft

import (
	"fmt"
	"github.com/dhiaayachi/multiraft/encoding"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"io"
)

//go:generate mockery --name RaftAdder --inpackage
type RaftAdder interface {
	addRaft(id uint32) error
}

type Fsm struct {
	fsm       raft.FSM
	logger    hclog.Logger
	id        raft.ServerID
	multiRaft RaftAdder
}

func NewFSM(fsm raft.FSM, r RaftAdder, logger hclog.Logger, id raft.ServerID) *Fsm {
	return &Fsm{fsm: fsm, multiRaft: r, logger: logger, id: id}
}

func (f *Fsm) Apply(log *raft.Log) interface{} {
	conf := &PartitionConfiguration{}
	err := encoding.DecodeMsgPack(log.Data, conf)
	if err != nil {
		f.logger.Error("failed to decode raft partition configuration", "error", err)
		return fmt.Errorf("decode raft partition configuration: %w", err)
	}
	if f.inServers(conf.Servers) {
		err := f.multiRaft.addRaft(conf.PartitionID)
		if err != nil {
			f.logger.Error("failed to add raft server", "error", err)
			return fmt.Errorf("failed to add raft server: %w", err)
		}
	}
	return f.fsm.Apply(log)
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
