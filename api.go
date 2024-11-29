package multiraft

import (
	"fmt"
	"github.com/dhiaayachi/multiraft/consts"
	"github.com/dhiaayachi/multiraft/encoding"
	"github.com/dhiaayachi/multiraft/store"
	"github.com/dhiaayachi/multiraft/transport"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"sync"
	"time"
)

// MultiRaft represents a multi-partition Raft cluster.
// It manages multiple Raft instances, each corresponding to a partition.
type MultiRaft struct {
	// Mapping of partition IDs to Raft instances
	rafts map[consts.PartitionType]*raft.Raft

	raftsLock sync.RWMutex

	// Raft configuration
	conf *raft.Config

	// Factory for creating FSMs
	fsmFactory FsmFactory

	// Factory for creating log stores
	logsFactory LogStoreFactory

	// Factory for creating stable stores
	stableFactory StableStoreFactory

	// Factory for creating snapshot stores
	snapsFactory SnapshotStoreFactory

	// Transport layer for communication
	trans *transport.MuxTransport

	// Logger instance
	logger hclog.Logger
}

// Leader checks if the local node is the leader of the specified partition.
// Returns true if the local node is the leader.
func (mr *MultiRaft) Leader(id consts.PartitionType) bool {
	mr.raftsLock.RLock()
	defer mr.raftsLock.RUnlock()
	_, serverID := mr.rafts[id].LeaderWithID()
	return serverID == mr.conf.LocalID
}

// NewMultiRaft creates a new MultiRaft instance.
// It initializes the ZeroPartition and sets up the necessary factories and configuration.
func NewMultiRaft(conf *raft.Config, fsmFactory FsmFactory, logsFactory LogStoreFactory, stableFactory StableStoreFactory, snapsFactory SnapshotStoreFactory, trans *transport.MuxTransport) (*MultiRaft, error) {

	// Get logger from the config or create a new one
	logger := getOrCreateLogger(conf)

	// Initialize MultiRaft instance
	multiRaft := &MultiRaft{
		conf:          conf,
		fsmFactory:    fsmFactory,
		logsFactory:   logsFactory,
		stableFactory: stableFactory,
		snapsFactory:  snapsFactory,
		trans:         trans,
		logger:        logger,
	}

	// Create the ZeroPartition
	r, err := multiRaft.createZeroPartition(conf, logsFactory, stableFactory, snapsFactory, trans.NewPartition(consts.ZeroPartition))
	if err != nil {
		return nil, err
	}

	// Store the ZeroPartition Raft instance
	multiRaft.raftsLock.Lock()
	defer multiRaft.raftsLock.Unlock()
	rafts := make(map[consts.PartitionType]*raft.Raft)
	rafts[consts.ZeroPartition] = r
	multiRaft.rafts = rafts

	return multiRaft, nil
}

// AddPartition adds a new partition to the cluster.
// It validates the servers and replicates the partition configuration via the ZeroPartition.
func (mr *MultiRaft) AddPartition(servers raft.Configuration, id consts.PartitionType) raft.Future {

	// Ensure the partition ID is not reserved
	if id == consts.ZeroPartition {
		return ErrorFuture{fmt.Errorf("partition %s is reserved", id)}
	}

	partServers := make([]raft.Server, 0)

	mr.raftsLock.RLock()
	defer mr.raftsLock.RUnlock()

	if len(mr.rafts) == 0 {
		return ErrorFuture{fmt.Errorf("multiraft is not initialized")}
	}
	ZeroConfiguration := mr.rafts[consts.ZeroPartition].GetConfiguration().Configuration()

	// Validate servers are part of the ZeroPartition
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
			return ErrorFuture{fmt.Errorf("server not part of the cluster %s", server)}
		}
	}

	// Create and replicate the partition configuration
	partConf := store.PartitionConfiguration{Servers: partServers, PartitionID: id}
	pack, err := encoding.EncodeMsgPack(partConf)
	if err != nil {
		mr.raftsLock.RUnlock()
		return ErrorFuture{err}
	}
	future := mr.rafts[consts.ZeroPartition].ApplyLog(raft.Log{Data: pack.Bytes()}, mr.conf.HeartbeatTimeout)
	// This need to be unlocked before

	return future
}

// NewPartition initializes a new Raft instance for a given partition.
func (mr *MultiRaft) NewPartition(partition consts.PartitionType) error {
	newTransport := mr.trans.NewPartition(partition)
	if mr.conf.Logger != nil {
		mr.conf.Logger = mr.conf.Logger.Named(fmt.Sprintf("raft-%s-%s", partition, mr.conf.LocalID))
	} else {
		mr.conf.Logger = hclog.Default().Named(fmt.Sprintf("raft-%s-%s", partition, mr.conf.LocalID))
	}
	newRaft, err := raft.NewRaft(mr.conf, mr.fsmFactory(), mr.logsFactory(), mr.stableFactory(), mr.snapsFactory(), newTransport)
	if err != nil {
		return err
	}
	mr.raftsLock.Lock()
	defer mr.raftsLock.Unlock()

	_, ok := mr.rafts[partition]
	if ok {
		return fmt.Errorf("partition %s already exist", partition)
	}
	mr.rafts[partition] = newRaft
	return nil
}

// BootstrapCluster initializes a new Raft cluster for a specified partition.
func (mr *MultiRaft) BootstrapCluster(conf raft.Configuration, partition consts.PartitionType) raft.Future {
	mr.raftsLock.RLock()
	defer mr.raftsLock.RUnlock()
	r, ok := mr.rafts[partition]
	if !ok {
		return &ErrorFuture{fmt.Errorf("partition %s do not exist", partition)}
	}
	return r.BootstrapCluster(conf)
}

// Apply applies a command to a specified partition.
// This method is a placeholder and is not yet implemented.
func (mr *MultiRaft) Apply(cmd []byte, timeout time.Duration, partition consts.PartitionType) raft.ApplyFuture {
	mr.raftsLock.RLock()
	defer mr.raftsLock.RUnlock()
	r, ok := mr.rafts[partition]
	if !ok {
		return &ErrorFuture{fmt.Errorf("partition %s do not exist", partition)}
	}
	return r.Apply(cmd, timeout)
}

// ApplyLog applies a log entry to a specified partition.
// This method is a placeholder and is not yet implemented.
func (mr *MultiRaft) ApplyLog(log raft.Log, timeout time.Duration, partition consts.PartitionType) raft.ApplyFuture {
	mr.raftsLock.RLock()
	defer mr.raftsLock.RUnlock()
	r, ok := mr.rafts[partition]
	if !ok {
		return &ErrorFuture{fmt.Errorf("partition %s do not exist", partition)}
	}
	return r.ApplyLog(log, timeout)
}

type ErrorFuture struct {
	err error
}

func (f ErrorFuture) Index() uint64 {
	return 0
}

func (f ErrorFuture) Response() interface{} {
	return f.err
}

func (f ErrorFuture) Error() error {
	return f.err
}
