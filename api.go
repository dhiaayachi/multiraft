package multiraft

import (
	"fmt"
	"github.com/dhiaayachi/multiraft/consts"
	"github.com/dhiaayachi/multiraft/encoding"
	"github.com/dhiaayachi/multiraft/store"
	"github.com/dhiaayachi/multiraft/transport"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"os"
	"sync/atomic"
	"time"
)

//go:generate mockery --name MultiRaft --inpackage
type MultiRaft struct {
	rafts         atomic.Pointer[map[consts.PartitionType]*raft.Raft]
	conf          *raft.Config
	fsmFactory    FsmFactory
	logsFactory   LogStoreFactory
	stableFactory StableStoreFactory
	snapsFactory  SnapshotStoreFactory
	trans         transport.Transport
	logger        hclog.Logger
}

func (r *MultiRaft) Leader(id consts.PartitionType) bool {
	rafts := *r.rafts.Load()
	_, serverID := rafts[id].LeaderWithID()
	return serverID == r.conf.LocalID
}

func NewMultiRaft(conf *raft.Config, fsmFactory FsmFactory, logsFactory LogStoreFactory, stableFactory StableStoreFactory, snapsFactory SnapshotStoreFactory, trans transport.Transport) (*MultiRaft, error) {

	// get logger from the config or create one
	logger := getOrCreateLogger(conf)

	//Create MultiRaft data struct and store the factories that will be used to create future raft instances.
	multiRaft := &MultiRaft{
		conf:          conf,
		fsmFactory:    fsmFactory,
		logsFactory:   logsFactory,
		stableFactory: stableFactory,
		snapsFactory:  snapsFactory,
		trans:         trans,
		logger:        logger,
	}

	// Create the ZeroPartition, this is safe here as each server need to create a  MultiRaft instance anyway
	r, err := multiRaft.createZeroPartition(conf, logsFactory, stableFactory, snapsFactory, trans.NewPartition(consts.ZeroPartition))
	if err != nil {
		return nil, err
	}

	// Store the raft instance at index zero, this need to be at index 0
	// 0 index is reserved for internal usage and can't be used by user.
	rafts := make(map[consts.PartitionType]*raft.Raft)
	rafts[consts.ZeroPartition] = r
	multiRaft.rafts.Store(&rafts)

	return multiRaft, nil
}

func (r *MultiRaft) AddPartition(servers raft.Configuration, id consts.PartitionType) error {

	// index start at 0, 0 is reserved for the "ZeroPartition"

	if id == consts.ZeroPartition {
		return fmt.Errorf("partition %s is reserved", id)
	}

	partServers := make([]raft.Server, 0)

	rafts := *r.rafts.Load()
	if len(rafts) == 0 {
		return fmt.Errorf("multiraft is not initialized")
	}
	ZeroConfiguration := rafts[consts.ZeroPartition].GetConfiguration().Configuration()

	// Check that the partition servers are part of the ZeroPartition
	// note that ZeroPartition is supposed to have all the servers
	for _, server := range servers.Servers {
		inConf := false
		for _, confServer := range ZeroConfiguration.Servers {
			if server.ID == confServer.ID {
				partServers = append(partServers, confServer)
				inConf = true
				break
			}
		}
		if !inConf {
			return fmt.Errorf("server not part of the cluster %s", server)
		}
	}

	// Create the partition configuration and store it in the ZeroPartition
	// This should trigger the replication of the configuration to all the servers
	partConf := store.PartitionConfiguration{Servers: partServers, PartitionID: id}
	pack, err := encoding.EncodeMsgPack(partConf)
	if err != nil {
		return err
	}
	future := rafts[consts.ZeroPartition].ApplyLog(raft.Log{Data: pack.Bytes()}, r.conf.HeartbeatTimeout)

	return future.Error()
}

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

func storePartition(rafts map[consts.PartitionType]*raft.Raft, id consts.PartitionType, r *raft.Raft) map[consts.PartitionType]*raft.Raft {
	raftsCopy := make(map[consts.PartitionType]*raft.Raft)
	for k, v := range rafts {
		raftsCopy[k] = v
	}
	raftsCopy[id] = r
	return raftsCopy
}

func (r *MultiRaft) createZeroPartition(conf *raft.Config, logsFactory LogStoreFactory, stableFactory StableStoreFactory, snapsFactory SnapshotStoreFactory, trans raft.Transport) (*raft.Raft, error) {

	if r.conf.Logger != nil {
		r.conf.Logger = r.conf.Logger.Named(fmt.Sprintf("raft-%d-%s", 0, r.conf.LocalID))
	} else {
		r.conf.Logger = hclog.Default().Named(fmt.Sprintf("raft-%d-%s", 0, r.conf.LocalID))
	}
	zeroFsm, _ := store.NewFSM(r, r.conf.Logger.With("id", conf.LocalID), conf.LocalID)
	return raft.NewRaft(conf, zeroFsm, logsFactory(), stableFactory(), snapsFactory(), trans)
}

func (r *MultiRaft) AddRaft(partition consts.PartitionType) error {
	newTransport := r.trans.NewPartition(partition)
	if r.conf.Logger != nil {
		r.conf.Logger = r.conf.Logger.Named(fmt.Sprintf("raft-%s-%s", partition, r.conf.LocalID))
	} else {
		r.conf.Logger = hclog.Default().Named(fmt.Sprintf("raft-%s-%s", partition, r.conf.LocalID))
	}
	newRaft, err := raft.NewRaft(r.conf, r.fsmFactory(), r.logsFactory(), r.stableFactory(), r.snapsFactory(), newTransport)
	if err != nil {
		return err
	}
	oldRafts := *r.rafts.Load()
	_, ok := oldRafts[partition]
	if ok {
		return fmt.Errorf("partition %s already exist", partition)
	}
	newRafts := storePartition(oldRafts, partition, newRaft)
	r.rafts.Store(&newRafts)
	//if !swapped {
	//	r.logger.Warn("not swapping raft store")
	//}
	return nil
}

func (r *MultiRaft) BootstrapCluster(conf raft.Configuration, partition consts.PartitionType) raft.Future {
	rafts := *r.rafts.Load()
	return rafts[partition].BootstrapCluster(conf)
}

func (r *MultiRaft) Apply(cmd []byte, timeout time.Duration, partition consts.PartitionType) raft.ApplyFuture {
	panic("implement me")
}
func (r *MultiRaft) ApplyLog(log raft.Log, timeout time.Duration, partition consts.PartitionType) raft.ApplyFuture {
	panic("implement me")
}
