package multiraft

import (
	"fmt"
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

func TestCreateCluster(t *testing.T) {

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
	f := mrs[0].mr.BootstrapCluster(configuration, 0)
	require.NoError(t, f.Error())
	rafts := *mrs[0].mr.rafts.Load()

	time.Sleep(100 * time.Millisecond)
	ch := rafts[0].LeaderCh()

	<-ch

	leader, _ := rafts[0].LeaderWithID()
	require.NotEmpty(t, leader)
	time.Sleep(100 * time.Millisecond)

	err = mrs[0].mr.AddPartition(configuration)

	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)

}
