package multiraft

import (
	"fmt"
	"github.com/dhiaayachi/multiraft/consts"
	"github.com/dhiaayachi/multiraft/transport"
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
		transport.NewMuxTransport(transportRaft),
	)
	require.NoError(t, err)
	require.NotNil(t, mr)
}

func TestCreateClusterAndPartition(t *testing.T) {

	create := func(i int) (*MultiRaft, error, raft.ServerAddress, raft.ServerID, *raft.InmemTransport) {
		config := raft.DefaultConfig()
		addr, transportRaft := raft.NewInmemTransport("")
		config.LocalID = raft.ServerID(fmt.Sprintf("id%d", i))
		multiRaft, err := NewMultiRaft(
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
			transport.NewMuxTransport(transportRaft),
		)
		return multiRaft, err, addr, config.LocalID, transportRaft

	}

	mrs := make([]struct {
		mr    *MultiRaft
		addr  raft.ServerAddress
		id    raft.ServerID
		trans *raft.InmemTransport
	}, 3)
	var err error
	for i := 0; i < 3; i++ {
		mrs[i].mr, err, mrs[i].addr, mrs[i].id, mrs[i].trans = create(i)
		require.NoError(t, err)
		require.NotNil(t, mrs[i])
	}
	for i := 0; i < 3; i++ {
		for j := 0; j < 3; j++ {
			if i == j {
				continue
			}
			mrs[i].trans.Connect(mrs[j].addr, mrs[j].trans)
		}
	}
	configuration := raft.Configuration{}
	for i := 0; i < 3; i++ {
		var err error
		require.NoError(t, err)
		configuration.Servers = append(configuration.Servers, raft.Server{
			ID:      mrs[i].id,
			Address: mrs[i].addr,
		})
	}
	f := mrs[0].mr.BootstrapCluster(configuration, consts.ZeroPartition)
	require.NoError(t, f.Error())

	WaitForLeader(t, mrs[0].mr, consts.ZeroPartition, 5*time.Second)

	rafts := *mrs[0].mr.rafts.Load()
	leader, _ := rafts[consts.ZeroPartition].LeaderWithID()
	require.NotEmpty(t, leader)

	err = mrs[0].mr.AddPartition(configuration, "default")

	require.NoError(t, err)

	WaitForLeader(t, mrs[0].mr, "default", 5*time.Second)
	rafts = *mrs[0].mr.rafts.Load()
	leader, _ = rafts["default"].LeaderWithID()
	require.NotEmpty(t, leader)

}

func TestCreateClusterAndPartitionInSomeNodes(t *testing.T) {

	create := func(i int) (*MultiRaft, error, raft.ServerAddress, raft.ServerID, *raft.InmemTransport) {
		config := raft.DefaultConfig()
		addr, transportRaft := raft.NewInmemTransport("")
		config.LocalID = raft.ServerID(fmt.Sprintf("id%d", i))
		multiRaft, err := NewMultiRaft(
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
			transport.NewMuxTransport(transportRaft),
		)
		return multiRaft, err, addr, config.LocalID, transportRaft

	}

	mrs := make([]struct {
		mr    *MultiRaft
		addr  raft.ServerAddress
		id    raft.ServerID
		trans *raft.InmemTransport
	}, 5)
	var err error
	for i := 0; i < 5; i++ {
		mrs[i].mr, err, mrs[i].addr, mrs[i].id, mrs[i].trans = create(i)
		require.NoError(t, err)
		require.NotNil(t, mrs[i])
	}
	for i := 0; i < 5; i++ {
		for j := 0; j < 5; j++ {
			if i == j {
				continue
			}
			mrs[i].trans.Connect(mrs[j].addr, mrs[j].trans)
		}
	}
	configuration := raft.Configuration{}
	for i := 0; i < 5; i++ {
		var err error
		require.NoError(t, err)
		configuration.Servers = append(configuration.Servers, raft.Server{
			ID:      mrs[i].id,
			Address: mrs[i].addr,
		})
	}
	f := mrs[0].mr.BootstrapCluster(configuration, consts.ZeroPartition)
	require.NoError(t, f.Error())

	WaitForLeader(t, mrs[0].mr, consts.ZeroPartition, 5*time.Second)

	rafts := *mrs[0].mr.rafts.Load()
	leader, _ := rafts[consts.ZeroPartition].LeaderWithID()
	require.NotEmpty(t, leader)

	partConfiguration := raft.Configuration{}
	for i := 2; i < 5; i++ {
		var err error
		require.NoError(t, err)
		partConfiguration.Servers = append(partConfiguration.Servers, raft.Server{
			ID:      mrs[i].id,
			Address: mrs[i].addr,
		})
	}

	time.Sleep(time.Second)
	err = mrs[2].mr.AddPartition(partConfiguration, "default")

	require.NoError(t, err)

	WaitForLeader(t, mrs[2].mr, "default", 5*time.Second)
	rafts = *mrs[2].mr.rafts.Load()
	leader, _ = rafts["default"].LeaderWithID()
	require.NotEmpty(t, leader)

}

func WaitForLeader(t *testing.T, mr *MultiRaft, partition consts.PartitionType, timeout time.Duration) {
	rafts := *mr.rafts.Load()

	ch := rafts[partition].LeaderCh()
	timeoutCh := time.After(timeout)
	select {
	case <-ch:
		return
	case <-timeoutCh:
		t.Fatal("timeout waiting for leader")

	}
}
