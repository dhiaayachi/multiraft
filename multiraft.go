package multiraft

import (
	"fmt"
	"github.com/dhiaayachi/multiraft/store"
	"github.com/dhiaayachi/raft"
	"github.com/hashicorp/go-hclog"
	"os"
)

type FsmFactory = func() raft.FSM
type LogStoreFactory = func() raft.LogStore
type StableStoreFactory = func() raft.StableStore
type SnapshotStoreFactory = func() raft.SnapshotStore

func getOrCreateLogger(conf *raft.Config) hclog.Logger {
	if conf.Logger != nil {
		return conf.Logger
	}
	if conf.LogOutput == nil {
		conf.LogOutput = os.Stderr
	}

	return hclog.New(&hclog.LoggerOptions{
		Name:   "raft",
		Level:  hclog.LevelFromString(conf.LogLevel),
		Output: conf.LogOutput,
	})
}

func (mr *MultiRaft) createZeroPartition(conf *raft.Config, logsFactory LogStoreFactory, stableFactory StableStoreFactory, snapsFactory SnapshotStoreFactory, trans raft.Transport) (*raft.Raft, error) {

	if mr.conf.Logger != nil {
		mr.conf.Logger = mr.conf.Logger.Named(fmt.Sprintf("raft-%d-%s", 0, mr.conf.LocalID))
	} else {
		mr.conf.Logger = hclog.Default().Named(fmt.Sprintf("raft-%d-%s", 0, mr.conf.LocalID))
	}
	zeroFsm, _ := store.NewFSM(mr, mr.conf.Logger.With("id", conf.LocalID), conf.LocalID)
	return raft.NewRaft(conf, zeroFsm, logsFactory(), stableFactory(), snapsFactory(), trans)
}
