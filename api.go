package multiraft

import (
	"fmt"
	"github.com/dhiaayachi/multiraft/encoding"
	"github.com/dhiaayachi/multiraft/store"
	"github.com/dhiaayachi/multiraft/transport"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"os"
	"sync/atomic"
)

const ZeroPartition = 0

//go:generate mockery --name MultiRaft --inpackage
type MultiRaft struct {
	rafts         atomic.Pointer[[]*raft.Raft]
	conf          *raft.Config
	fsmFactory    FsmFactory
	logsFactory   LogStoreFactory
	stableFactory StableStoreFactory
	snapsFactory  SnapshotStoreFactory
	trans         transport.Transport
	logger        hclog.Logger
	partIdx       atomic.Uint32
}

func (r *MultiRaft) Leader(id uint32) bool {
	rafts := *r.rafts.Load()
	addr, serverID := rafts[id].LeaderWithID()
	fmt.Printf("dhayachi:: addr%s\n", addr)
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
	r, err := multiRaft.createZeroPartition(conf, logsFactory, stableFactory, snapsFactory, trans.NewPartition(ZeroPartition))
	if err != nil {
		return nil, err
	}

	// Store the raft instance at index zero, this need to be at index 0
	// 0 index is reserved for internal usage and can't be used by user.
	rafts := make([]*raft.Raft, 0)
	rafts = append(rafts, r)
	multiRaft.rafts.Store(&rafts)

	return multiRaft, nil
}

func (r *MultiRaft) AddPartition(servers raft.Configuration) error {

	// index start at 0, 0 is reserved for the "ZeroPartition"
	u := r.partIdx.Add(1)
	if u < 1 {
		return fmt.Errorf("multiraft is not initialized")
	}

	partServers := make([]raft.Server, 0)

	rafts := *r.rafts.Load()
	ZeroConfiguration := rafts[ZeroPartition].GetConfiguration().Configuration()

	// Check that the partition servers are part of the ZeroPartition
	// ZeroPartition is supposed to have all the servers
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
	// TODO: each server when receiving the config need to check if it's part of the partition
	//  and create a new raft instance if so. This could be done as part of the FSM of the ZeroPartition
	// At that point we can also store more metadata about the partition.
	partConf := store.PartitionConfiguration{Servers: partServers, PartitionID: u}
	pack, err := encoding.EncodeMsgPack(partConf)
	if err != nil {
		return err
	}
	future := rafts[ZeroPartition].ApplyLog(raft.Log{Data: pack.Bytes()}, r.conf.HeartbeatTimeout)

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

func storePartition(rafts []*raft.Raft, r *raft.Raft) []*raft.Raft {
	raftsCopy := make([]*raft.Raft, 0)
	raftsCopy = append(raftsCopy, rafts...)
	raftsCopy = append(raftsCopy, r)
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

func (r *MultiRaft) AddRaft(partition uint32) error {
	newTransport := r.trans.NewPartition(partition)
	if r.conf.Logger != nil {
		r.conf.Logger = r.conf.Logger.Named(fmt.Sprintf("raft-%d-%s", partition, r.conf.LocalID))
	} else {
		r.conf.Logger = hclog.Default().Named(fmt.Sprintf("raft-%d-%s", partition, r.conf.LocalID))
	}
	newRaft, err := raft.NewRaft(r.conf, r.fsmFactory(), r.logsFactory(), r.stableFactory(), r.snapsFactory(), newTransport)
	if err != nil {
		return err
	}
	oldRafts := r.rafts.Load()
	newRafts := storePartition(*oldRafts, newRaft)
	r.rafts.CompareAndSwap(oldRafts, &newRafts)
	return nil
}

func (r *MultiRaft) BootstrapCluster(conf raft.Configuration, partition uint32) raft.Future {
	rafts := *r.rafts.Load()
	return rafts[partition].BootstrapCluster(conf)
}
