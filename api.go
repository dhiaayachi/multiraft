package multiraft

import (
	"fmt"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
)

type MultiRaft struct {
	rafts         map[uint64]*raft.Raft
	conf          *raft.Config
	fsmFactory    FsmFactory
	logsFactory   LogStoreFactory
	stableFactory StableStoreFactory
	snapsFactory  SnapshotStoreFactory
	trans         Transport
	logger        hclog.Logger
}

func (r MultiRaft) AddPartition(u uint64) error {
	if _, ok := r.rafts[u]; ok {
		return fmt.Errorf("partition %d already exists", u)
	}
	part, err := createPartition(r.conf, r.fsmFactory, r.logsFactory, r.stableFactory, r.snapsFactory, r.trans.RaftTransport(u))
	if err != nil {
		return err
	}
	r.rafts[u] = part
	return nil
}

type FsmFactory = func() raft.FSM
type LogStoreFactory = func() raft.LogStore
type StableStoreFactory = func() raft.StableStore
type SnapshotStoreFactory = func() raft.SnapshotStore

func NewMultiRaft(conf *raft.Config, fsmFactory FsmFactory, logsFactory LogStoreFactory, stableFactory StableStoreFactory, snapsFactory SnapshotStoreFactory, trans Transport) (*MultiRaft, error) {

	multiRaft := MultiRaft{
		conf:          conf,
		fsmFactory:    fsmFactory,
		logsFactory:   logsFactory,
		stableFactory: stableFactory,
		snapsFactory:  snapsFactory,
		trans:         trans,
	}

	err := multiRaft.AddPartition(0)
	if err != nil {
		return nil, err
	}
	return &multiRaft, nil
}
func createPartition(conf *raft.Config, fsmFactory FsmFactory, logsFactory LogStoreFactory, stableFactory StableStoreFactory, snapsFactory SnapshotStoreFactory, trans raft.Transport) (*raft.Raft, error) {
	return raft.NewRaft(conf, fsmFactory(), logsFactory(), stableFactory(), snapsFactory(), trans)
}
