package multiraft

import (
	"github.com/dhiaayachi/multiraft/transport"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
	"testing"
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
