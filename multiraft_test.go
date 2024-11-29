package multiraft

import (
	"github.com/dhiaayachi/multiraft/consts"
	"github.com/dhiaayachi/multiraft/transport"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
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
	f := c.mr[0].BootstrapCluster(configuration, consts.ZeroPartition)
	require.NoError(t, f.Error())

	err = waitForLeader(c, consts.ZeroPartition)
	require.NoError(t, err)

	rafts := c.mr[0].rafts
	leader, _ := rafts[consts.ZeroPartition].LeaderWithID()
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
	f := c.mr[0].BootstrapCluster(configuration, consts.ZeroPartition)
	require.NoError(t, f.Error())

	err = waitForLeader(c, consts.ZeroPartition)
	require.NoError(t, err)

	rafts := c.mr[0].rafts
	leader, _ := rafts[consts.ZeroPartition].LeaderWithID()
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
	f = c.mr[0].AddPartition(partConfig, "default")

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
	f := c.mr[0].BootstrapCluster(configuration, consts.ZeroPartition)
	require.NoError(t, f.Error())

	err = waitForLeader(c, consts.ZeroPartition)
	require.NoError(t, err)

	// Apply a command
	cmd := []byte("test_command")
	future := c.mr[0].Apply(cmd, time.Second, consts.ZeroPartition)
	require.NoError(t, future.Error())

	var lr *MultiRaft
	for _, mr := range c.mr {
		if mr.Leader(consts.ZeroPartition) {
			lr = mr
		}
	}

	require.NotNil(t, lr)
	// Apply a command on non-existing partition
	future = lr.Apply(cmd, time.Second, "foo")
	require.Error(t, future.Error())

	f = c.mr[0].AddPartition(configuration, "foo")
	require.NoError(t, f.Error())
	// Apply a command on existing partition

	err = waitForLeader(c, "foo")

	var flr *MultiRaft
	for _, mr := range c.mr {
		if mr.Leader(consts.ZeroPartition) {
			flr = mr
		}
	}
	require.NotNil(t, lr)

	future = flr.Apply(cmd, time.Second, "foo")
	require.NoError(t, future.Error())

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
	f := c.mr[0].BootstrapCluster(configuration, consts.ZeroPartition)
	require.NoError(t, f.Error())

	err = waitForLeader(c, consts.ZeroPartition)
	require.NoError(t, err)

	// Apply a command
	cmd := []byte("test_command")
	future := c.mr[0].Apply(cmd, time.Second, consts.ZeroPartition)
	require.NoError(t, future.Error())

	var lr *MultiRaft
	for _, mr := range c.mr {
		if mr.Leader(consts.ZeroPartition) {
			lr = mr
		}
	}

	require.NotNil(t, lr)
	// Apply a command on non-existing partition
	future = lr.ApplyLog(raft.Log{Data: cmd}, time.Second, "foo")
	require.Error(t, future.Error())

	f = c.mr[0].AddPartition(configuration, "foo")
	require.NoError(t, f.Error())
	// Apply a command on existing partition

	err = waitForLeader(c, "foo")

	var flr *MultiRaft
	for _, mr := range c.mr {
		if mr.Leader(consts.ZeroPartition) {
			flr = mr
		}
	}
	require.NotNil(t, lr)

	future = flr.ApplyLog(raft.Log{Data: cmd}, time.Second, "foo")
	require.NoError(t, future.Error())

}
