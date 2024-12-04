package multiraft

import (
	"fmt"
	"github.com/dhiaayachi/multiraft/encoding"
	"github.com/dhiaayachi/multiraft/partition"
	"github.com/dhiaayachi/multiraft/store"
	"github.com/dhiaayachi/multiraft/transport"
	"github.com/dhiaayachi/raft"
	"github.com/hashicorp/go-hclog"
	"sync"
	"time"
)

// MultiRaft represents a multi-partition Raft cluster.
// It manages multiple Raft instances, each corresponding to a partition.
type MultiRaft struct {
	// Mapping of partition IDs to Raft instances
	rafts map[partition.Typ]*raft.Raft

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
func (mr *MultiRaft) Leader(id partition.Typ) bool {
	mr.raftsLock.RLock()
	defer mr.raftsLock.RUnlock()
	_, serverID := mr.rafts[id].LeaderWithID()
	return serverID == mr.conf.LocalID
}

// NewMultiRaft creates a new MultiRaft instance.
// It initializes the Zero and sets up the necessary factories and configuration.
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

	// Create the Zero
	r, err := multiRaft.createZeroPartition(conf, logsFactory, stableFactory, snapsFactory, trans.NewPartition(partition.Zero))
	if err != nil {
		return nil, err
	}

	// Store the Zero Raft instance
	multiRaft.raftsLock.Lock()
	defer multiRaft.raftsLock.Unlock()
	rafts := make(map[partition.Typ]*raft.Raft)
	rafts[partition.Zero] = r
	multiRaft.rafts = rafts

	return multiRaft, nil
}

// AddPartition adds a new partition to the cluster.
// It validates the servers and replicates the partition configuration via the Zero.
func (mr *MultiRaft) AddPartition(servers raft.Configuration, id partition.Typ) raft.Future {

	// Ensure the partition ID is not reserved
	if id == partition.Zero {
		return ErrorFuture{fmt.Errorf("partition %s is reserved", id)}
	}

	partServers := make([]raft.Server, 0)

	mr.raftsLock.RLock()
	defer mr.raftsLock.RUnlock()

	if len(mr.rafts) == 0 {
		return ErrorFuture{fmt.Errorf("multiraft is not initialized")}
	}
	ZeroConfiguration := mr.rafts[partition.Zero].GetConfiguration().Configuration()

	// Validate servers are part of the Zero
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
	future := mr.rafts[partition.Zero].ApplyLog(raft.Log{Data: pack.Bytes()}, mr.conf.HeartbeatTimeout)
	// This need to be unlocked before

	return future
}

// NewPartition initializes a new Raft instance for a given partition.
func (mr *MultiRaft) NewPartition(partition partition.Typ) error {
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
func (mr *MultiRaft) BootstrapCluster(conf raft.Configuration, partition partition.Typ) raft.Future {
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
func (mr *MultiRaft) Apply(cmd []byte, timeout time.Duration, partition partition.Typ) raft.ApplyFuture {
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
func (mr *MultiRaft) ApplyLog(log raft.Log, timeout time.Duration, partition partition.Typ) raft.ApplyFuture {
	mr.raftsLock.RLock()
	defer mr.raftsLock.RUnlock()
	r, ok := mr.rafts[partition]
	if !ok {
		return &ErrorFuture{fmt.Errorf("partition %s do not exist", partition)}
	}
	return r.ApplyLog(log, timeout)
}

func (mr *MultiRaft) Shutdown() raft.Future {
	mr.raftsLock.RLock()
	defer mr.raftsLock.RUnlock()
	for _, r := range mr.rafts {
		f := r.Shutdown()
		if f.Error() != nil {
			return &ErrorFuture{f.Error()}
		}
	}
	return &ErrorFuture{}
}

func (mr *MultiRaft) LeadershipTransferToServer(id raft.ServerID, address raft.ServerAddress, partition partition.Typ) raft.Future {
	mr.raftsLock.RLock()
	defer mr.raftsLock.RUnlock()
	if _, ok := mr.rafts[partition]; !ok {
		return &ErrorFuture{fmt.Errorf("partition %s do not exist", partition)}
	}
	return mr.rafts[partition].LeadershipTransferToServer(id, address)
}
func (mr *MultiRaft) LeadershipTransfer(partition partition.Typ) raft.Future {
	mr.raftsLock.RLock()
	defer mr.raftsLock.RUnlock()
	if _, ok := mr.rafts[partition]; !ok {
		return &ErrorFuture{fmt.Errorf("partition %s do not exist", partition)}
	}
	return mr.rafts[partition].LeadershipTransfer()
}

func (mr *MultiRaft) RemoveServer(id raft.ServerID, prevIndex uint64, timeout time.Duration) raft.IndexFuture {
	mr.raftsLock.RLock()
	defer mr.raftsLock.RUnlock()
	for p, r := range mr.rafts {
		if p == partition.Zero {
			continue
		}
		f := r.RemoveServer(id, prevIndex, timeout)
		if f.Error() != nil {
			return &ErrorFuture{f.Error()}
		}
	}
	return mr.rafts[partition.Zero].RemoveServer(id, prevIndex, timeout)
}

func (mr *MultiRaft) GetConfiguration(partition partition.Typ) raft.ConfigurationFuture {
	mr.raftsLock.RLock()
	defer mr.raftsLock.RUnlock()
	if _, ok := mr.rafts[partition]; !ok {
		return &ErrorFuture{fmt.Errorf("partition %s do not exist", partition)}
	}
	return mr.rafts[partition].GetConfiguration()
}

func (mr *MultiRaft) State(partition partition.Typ) raft.RaftState {
	mr.raftsLock.RLock()
	defer mr.raftsLock.RUnlock()
	if _, ok := mr.rafts[partition]; !ok {
		return raft.Shutdown
	}
	return mr.rafts[partition].State()
}

func (mr *MultiRaft) AddVoter(id raft.ServerID, address raft.ServerAddress, prevIndex uint64, timeout time.Duration) raft.IndexFuture {
	mr.raftsLock.RLock()
	defer mr.raftsLock.RUnlock()
	if _, ok := mr.rafts[partition.Zero]; !ok {
		return &ErrorFuture{fmt.Errorf("partition %s do not exist", partition.Zero)}
	}
	return mr.rafts[partition.Zero].AddVoter(id, address, prevIndex, timeout)
}

func (mr *MultiRaft) AddVoterToPartition(id raft.ServerID, address raft.ServerAddress, prevIndex uint64, timeout time.Duration, part partition.Typ) raft.IndexFuture {
	mr.raftsLock.RLock()
	defer mr.raftsLock.RUnlock()
	if part == partition.Zero {
		return mr.AddVoter(id, address, prevIndex, timeout)
	}

	if _, ok := mr.rafts[partition.Zero]; !ok {
		return &ErrorFuture{fmt.Errorf("partition %s do not exist", partition.Zero)}
	}
	config := mr.rafts[partition.Zero].GetConfiguration()
	inConf := false
	for _, c := range config.Configuration().Servers {
		if c.ID == id {
			inConf = true
			break
		}
	}
	if !inConf {
		return &ErrorFuture{fmt.Errorf("Voter need to be in zero partition (%s) before added to any other partition", partition.Zero)}
	}
	if _, ok := mr.rafts[part]; !ok {
		return &ErrorFuture{fmt.Errorf("partition %s do not exist", partition.Zero)}
	}
	return mr.rafts[part].AddVoter(id, address, prevIndex, timeout)
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
func (f ErrorFuture) Configuration() raft.Configuration {
	return raft.Configuration{}
}
