package store

import (
	"fmt"
	"github.com/dhiaayachi/multiraft/consts"
	"github.com/dhiaayachi/multiraft/encoding"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"io"
)

//go:generate mockery --name RaftAdder --inpackage
type RaftAdder interface {
	AddRaft(id consts.PartitionType) error
	BootstrapCluster(conf raft.Configuration, partition consts.PartitionType) raft.Future
}

type PartitionConfiguration struct {
	PartitionID consts.PartitionType
	Servers     []raft.Server
}

type Fsm struct {
	fsm       raft.FSM
	logger    hclog.Logger
	id        raft.ServerID
	raftAdder RaftAdder
}

func NewFSM(r RaftAdder, logger hclog.Logger, id raft.ServerID) (*Fsm, error) {
	state, err := NewPartitionState()
	if err != nil {
		return nil, err
	}
	return &Fsm{fsm: state, raftAdder: r, logger: logger, id: id}, nil
}

func (f *Fsm) Apply(log *raft.Log) interface{} {
	conf := &PartitionConfiguration{}
	err := encoding.DecodeMsgPack(log.Data, conf)
	if err != nil {
		f.logger.Error("failed to decode raft partition configuration", "error", err)
		return fmt.Errorf("decode raft partition configuration: %w", err)
	}
	if f.inServers(conf.Servers) {
		f.logger.Info("adding new raft partition", "part-id", conf.PartitionID, "servers", conf.Servers)
		err := f.raftAdder.AddRaft(conf.PartitionID)
		if err != nil {
			f.logger.Error("failed to add raft server", "error", err)
			return fmt.Errorf("failed to add raft server: %w", err)
		}

		if f.id == conf.Servers[0].ID {
			f.logger.Info("bootstrapping new raft partition", "part-id", conf.PartitionID, "servers", conf.Servers)
			future := f.raftAdder.BootstrapCluster(raft.Configuration{Servers: conf.Servers}, conf.PartitionID)
			if err := future.Error(); err != nil {
				return err
			}
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
