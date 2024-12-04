package multiraft

import (
	"github.com/dhiaayachi/multiraft/partition"
	"github.com/dhiaayachi/multiraft/transport"
	"github.com/dhiaayachi/raft"
	"github.com/hashicorp/go-hclog"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestCreateMultiRaft(t *testing.T) {
	_, transportRaft := raft.NewInmemTransport("")

	config := raft.DefaultConfig()
	config.LocalID = "id1"
	mr, err := NewMultiRaft(
		config,
		func() raft.FSM {
			return &raft.MockFSM{}
		},
		func() raft.LogStore {
			return raft.NewInmemStore()
		},
		func() raft.StableStore {
			return raft.NewInmemStore()
		},
		func() raft.SnapshotStore {
			return raft.NewDiscardSnapshotStore()
		},
		transport.NewMuxTransport(transportRaft, hclog.Default()),
	)
	require.NoError(t, err)
	require.NotNil(t, mr)
}

func TestCreateClusterAndPartition(t *testing.T) {

	numNodes := 3
	c, err := createCluster(t, numNodes)
	require.NoError(t, err)
	for i := 0; i < numNodes; i++ {
		for j := 0; j < numNodes; j++ {
			if i == j {
				continue
			}
			c.trans[i].Connect(c.addr[j], c.trans[j])
		}
	}
	configuration := raft.Configuration{}
	for i := 0; i < numNodes; i++ {
		var err error
		require.NoError(t, err)
		configuration.Servers = append(configuration.Servers, raft.Server{
			ID:      c.id[i],
			Address: c.addr[i],
		})
	}
	f := c.mr[0].BootstrapCluster(configuration, partition.Zero)
	require.NoError(t, f.Error())

	err = waitForLeader(c, partition.Zero)
	require.NoError(t, err)

	rafts := c.mr[0].rafts
	leader, _ := rafts[partition.Zero].LeaderWithID()
	require.NotEmpty(t, leader)

	f = c.mr[0].AddPartition(configuration, "default")
	require.NoError(t, f.Error())

	err = waitForLeader(c, "default")
	require.NoError(t, err)
	rafts = c.mr[0].rafts
	leader, _ = rafts["default"].LeaderWithID()
	require.NotEmpty(t, leader)

}

func TestCreateClusterAndPartitionInSomeNodes(t *testing.T) {

	numNodes := 5
	c, err := createCluster(t, numNodes)
	require.NoError(t, err)
	for i := 0; i < numNodes; i++ {
		for j := 0; j < numNodes; j++ {
			if i == j {
				continue
			}
			c.trans[i].Connect(c.addr[j], c.trans[j])
		}
	}
	configuration := raft.Configuration{}
	for i := 0; i < numNodes; i++ {
		var err error
		require.NoError(t, err)
		configuration.Servers = append(configuration.Servers, raft.Server{
			ID:      c.id[i],
			Address: c.addr[i],
		})
	}
	f := c.mr[0].BootstrapCluster(configuration, partition.Zero)
	require.NoError(t, f.Error())

	err = waitForLeader(c, partition.Zero)
	flr := c.partitionLeader(partition.Zero)
	require.NoError(t, err)

	rafts := flr.rafts
	leader, _ := rafts[partition.Zero].LeaderWithID()
	require.NotEmpty(t, leader)

	partConfig := raft.Configuration{}
	for i := 0; i < numNodes-2; i++ {
		var err error
		require.NoError(t, err)
		partConfig.Servers = append(partConfig.Servers, raft.Server{
			ID:      c.id[i],
			Address: c.addr[i],
		})
	}
	f = flr.AddPartition(partConfig, "default")

	require.NoError(t, f.Error())

	err = waitForLeader(c, "default")
	require.NoError(t, err)
	rafts = c.mr[0].rafts
	leader, _ = rafts["default"].LeaderWithID()
	require.NotEmpty(t, leader)

}

func TestApply(t *testing.T) {
	numNodes := 3
	c, err := createCluster(t, numNodes)
	require.NoError(t, err)
	for i := 0; i < numNodes; i++ {
		for j := 0; j < numNodes; j++ {
			if i == j {
				continue
			}
			c.trans[i].Connect(c.addr[j], c.trans[j])
		}
	}
	configuration := raft.Configuration{}
	for i := 0; i < numNodes; i++ {
		configuration.Servers = append(configuration.Servers, raft.Server{
			ID:      c.id[i],
			Address: c.addr[i],
		})
	}
	f := c.mr[0].BootstrapCluster(configuration, partition.Zero)
	require.NoError(t, f.Error())

	err = waitForLeader(c, partition.Zero)
	require.NoError(t, err)

	// Apply a command
	cmd := []byte("test_command")
	future := c.mr[0].Apply(cmd, time.Second, partition.Zero)
	require.NoError(t, future.Error())

	lr := c.partitionLeader(partition.Zero)

	require.NotNil(t, lr)
	// Apply a command on non-existing partition
	future = lr.Apply(cmd, time.Second, "foo")
	require.Error(t, future.Error())

	f = c.mr[0].AddPartition(configuration, "foo")
	require.NoError(t, f.Error())
	// Apply a command on existing partition

	err = waitForLeader(c, "foo")

	flr := c.partitionLeader("foo")
	require.NotNil(t, lr)

	future = flr.Apply(cmd, time.Second, "foo")
	require.NoError(t, future.Error())

}

func (c *cluster) partitionLeader(part partition.Typ) *MultiRaft {
	var flr *MultiRaft
	for _, mr := range c.mr {
		if mr.Leader(part) {
			flr = mr
		}
	}
	return flr
}

func TestApplyLog(t *testing.T) {
	numNodes := 3
	c, err := createCluster(t, numNodes)
	require.NoError(t, err)
	for i := 0; i < numNodes; i++ {
		for j := 0; j < numNodes; j++ {
			if i == j {
				continue
			}
			c.trans[i].Connect(c.addr[j], c.trans[j])
		}
	}
	configuration := raft.Configuration{}
	for i := 0; i < numNodes; i++ {
		configuration.Servers = append(configuration.Servers, raft.Server{
			ID:      c.id[i],
			Address: c.addr[i],
		})
	}
	f := c.mr[0].BootstrapCluster(configuration, partition.Zero)
	require.NoError(t, f.Error())

	err = waitForLeader(c, partition.Zero)
	require.NoError(t, err)

	// Apply a command
	cmd := []byte("test_command")
	future := c.mr[0].Apply(cmd, time.Second, partition.Zero)
	require.NoError(t, future.Error())

	lr := c.partitionLeader(partition.Zero)

	require.NotNil(t, lr)
	// Apply a command on non-existing partition
	future = lr.ApplyLog(raft.Log{Data: cmd}, time.Second, "foo")
	require.Error(t, future.Error())

	f = c.mr[0].AddPartition(configuration, "foo")
	require.NoError(t, f.Error())
	// Apply a command on existing partition

	err = waitForLeader(c, "foo")

	flr := c.partitionLeader(partition.Zero)
	require.NotNil(t, lr)

	future = flr.ApplyLog(raft.Log{Data: cmd}, time.Second, "foo")
	require.NoError(t, future.Error())

}

func TestLeaderAndState(t *testing.T) {
	numNodes := 3
	c, err := createCluster(t, numNodes)
	require.NoError(t, err)
	for i := 0; i < numNodes; i++ {
		for j := 0; j < numNodes; j++ {
			if i == j {
				continue
			}
			c.trans[i].Connect(c.addr[j], c.trans[j])
		}
	}

	configuration := raft.Configuration{}
	for i := 0; i < numNodes; i++ {
		configuration.Servers = append(configuration.Servers, raft.Server{
			ID:      c.id[i],
			Address: c.addr[i],
		})
	}

	f := c.mr[0].BootstrapCluster(configuration, partition.Zero)
	require.NoError(t, f.Error())

	err = waitForLeader(c, partition.Zero)
	require.NoError(t, err)

	// Test Leader method
	var leaderFound bool
	for _, mr := range c.mr {
		if mr.Leader(partition.Zero) {
			leaderFound = true
			break
		}
	}
	require.True(t, leaderFound)

	// Test State method
	for _, mr := range c.mr {
		state := mr.State(partition.Zero)
		require.NotEqual(t, raft.Shutdown, state)
	}

	// Test State for non-existent partition
	state := c.mr[0].State("non_existent")
	require.Equal(t, raft.Shutdown, state)
}

func TestNewPartition(t *testing.T) {
	numNodes := 3
	c, err := createCluster(t, numNodes)
	require.NoError(t, err)

	for i := 0; i < numNodes; i++ {
		for j := 0; j < numNodes; j++ {
			if i == j {
				continue
			}
			c.trans[i].Connect(c.addr[j], c.trans[j])
		}
	}

	configuration := raft.Configuration{}
	for i := 0; i < numNodes; i++ {
		configuration.Servers = append(configuration.Servers, raft.Server{
			ID:      c.id[i],
			Address: c.addr[i],
		})
	}

	f := c.mr[0].BootstrapCluster(configuration, partition.Zero)
	require.NoError(t, f.Error())

	err = waitForLeader(c, partition.Zero)
	require.NoError(t, err)

	flr := c.partitionLeader(partition.Zero)

	// Test creating a new partition
	err = flr.NewPartition("test_partition")
	require.NoError(t, err)

	// Test creating an existing partition
	err = flr.NewPartition("test_partition")
	require.Error(t, err)
}

func TestRemoveServer(t *testing.T) {
	numNodes := 3
	c, err := createCluster(t, numNodes)
	require.NoError(t, err)

	for i := 0; i < numNodes; i++ {
		for j := 0; j < numNodes; j++ {
			if i == j {
				continue
			}
			c.trans[i].Connect(c.addr[j], c.trans[j])
		}
	}

	configuration := raft.Configuration{}
	for i := 0; i < numNodes; i++ {
		configuration.Servers = append(configuration.Servers, raft.Server{
			ID:      c.id[i],
			Address: c.addr[i],
		})
	}

	f := c.mr[0].BootstrapCluster(configuration, partition.Zero)
	require.NoError(t, f.Error())

	err = waitForLeader(c, partition.Zero)
	require.NoError(t, err)

	flr := c.partitionLeader(partition.Zero)
	// Remove a server
	indexFuture := flr.RemoveServer(c.id[0], 0, time.Second)
	require.NoError(t, indexFuture.Error())
}

func TestLeadershipTransfer(t *testing.T) {
	numNodes := 3
	c, err := createCluster(t, numNodes)
	require.NoError(t, err)

	for i := 0; i < numNodes; i++ {
		for j := 0; j < numNodes; j++ {
			if i == j {
				continue
			}
			c.trans[i].Connect(c.addr[j], c.trans[j])
		}
	}

	configuration := raft.Configuration{}
	for i := 0; i < numNodes; i++ {
		configuration.Servers = append(configuration.Servers, raft.Server{
			ID:      c.id[i],
			Address: c.addr[i],
		})
	}

	f := c.mr[0].BootstrapCluster(configuration, partition.Zero)
	require.NoError(t, f.Error())

	err = waitForLeader(c, partition.Zero)
	require.NoError(t, err)
	flr := c.partitionLeader(partition.Zero)

	// Leadership transfer to specific server
	leaderTransferFuture := flr.LeadershipTransferToServer(
		c.id[1],
		c.addr[1],
		partition.Zero,
	)
	require.NoError(t, leaderTransferFuture.Error())

	require.NoError(t, waitForLeader(c, partition.Zero))

	flr = c.partitionLeader(partition.Zero)

	require.NotNil(t, flr)

	// Leadership transfer without specifying server
	leaderTransferFuture = flr.LeadershipTransfer(partition.Zero)
	require.NoError(t, leaderTransferFuture.Error())

	// Test leadership transfer on non-existent partition
	leaderTransferFuture = flr.LeadershipTransfer("non_existent")
	require.Error(t, leaderTransferFuture.Error())
}

func TestGetConfiguration(t *testing.T) {
	numNodes := 3
	c, err := createCluster(t, numNodes)
	require.NoError(t, err)

	for i := 0; i < numNodes; i++ {
		for j := 0; j < numNodes; j++ {
			if i == j {
				continue
			}
			c.trans[i].Connect(c.addr[j], c.trans[j])
		}
	}

	configuration := raft.Configuration{}
	for i := 0; i < numNodes; i++ {
		configuration.Servers = append(configuration.Servers, raft.Server{
			ID:      c.id[i],
			Address: c.addr[i],
		})
	}

	f := c.mr[0].BootstrapCluster(configuration, partition.Zero)
	require.NoError(t, f.Error())

	err = waitForLeader(c, partition.Zero)
	require.NoError(t, err)

	flr := c.partitionLeader(partition.Zero)

	// Get configuration for existing partition
	configFuture := flr.GetConfiguration(partition.Zero)
	require.NoError(t, configFuture.Error())

	// Get configuration for non-existent partition
	configFuture = flr.GetConfiguration("non_existent")
	require.Error(t, configFuture.Error())
}

func TestShutdown(t *testing.T) {
	numNodes := 3
	c, err := createCluster(t, numNodes)
	require.NoError(t, err)

	for i := 0; i < numNodes; i++ {
		for j := 0; j < numNodes; j++ {
			if i == j {
				continue
			}
			c.trans[i].Connect(c.addr[j], c.trans[j])
		}
	}

	configuration := raft.Configuration{}
	for i := 0; i < numNodes; i++ {
		configuration.Servers = append(configuration.Servers, raft.Server{
			ID:      c.id[i],
			Address: c.addr[i],
		})
	}

	f := c.mr[0].BootstrapCluster(configuration, partition.Zero)
	require.NoError(t, f.Error())

	err = waitForLeader(c, partition.Zero)
	require.NoError(t, err)

	// Shutdown MultiRaft
	shutdownFuture := c.mr[0].Shutdown()
	require.NoError(t, shutdownFuture.Error())
}

func TestAddPartitionErrors(t *testing.T) {
	// Uninitialized MultiRaft
	_, inmemTransport := raft.NewInmemTransport("")
	config := raft.DefaultConfig()
	config.LocalID = "id1"
	mr, err := NewMultiRaft(
		config,
		func() raft.FSM { return &raft.MockFSM{} },
		func() raft.LogStore { return raft.NewInmemStore() },
		func() raft.StableStore { return raft.NewInmemStore() },
		func() raft.SnapshotStore { return raft.NewDiscardSnapshotStore() },
		transport.NewMuxTransport(inmemTransport, hclog.Default()),
	)
	require.NoError(t, err)

	// Try to add Zero (reserved)
	configuration := raft.Configuration{
		Servers: []raft.Server{
			{ID: "test_id", Address: "test_addr"},
		},
	}
	future := mr.AddPartition(configuration, partition.Zero)
	require.Error(t, future.Error())

	// Add partition with server not in zero partition
	future = mr.AddPartition(configuration, "test_partition")
	require.Error(t, future.Error())
}
