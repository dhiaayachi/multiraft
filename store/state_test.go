package store

import (
	"github.com/dhiaayachi/multiraft/encoding"
	"github.com/dhiaayachi/multiraft/store/mocks"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCreateStateAndApply(t *testing.T) {
	// Create a state
	s, err := NewPartitionState()
	state := s.(*PartitionState)
	require.Nil(t, err)
	require.NotNil(t, state)

	//Apply a log
	applyPartition(t, state, 1)

	// Apply another log
	applyPartition(t, state, 2)

	// Read the first entry and check it's ok
	iter, err := state.db.Txn(false).Get(partitionTable, indexID, uint32(1))

	require.NoError(t, err)

	c := iter.Next().(*PartitionConfiguration)
	require.NotNil(t, c)

	require.Equal(t, c.PartitionID, uint32(1))

	require.Nil(t, iter.Next())

	// Read all the entries
	iter, err = state.db.Txn(false).Get(partitionTable, indexID)

	require.NoError(t, err)

	c = iter.Next().(*PartitionConfiguration)
	require.NotNil(t, c)
	require.Equal(t, c.PartitionID, uint32(1))

	c = iter.Next().(*PartitionConfiguration)
	require.NotNil(t, c)
	require.Equal(t, c.PartitionID, uint32(2))

	require.Nil(t, iter.Next())
}

func applyPartition(t *testing.T, state *PartitionState, id uint32) {
	pack, err := encoding.EncodeMsgPack(PartitionConfiguration{
		PartitionID: id,
	})
	require.Nil(t, err)
	ret := state.Apply(&raft.Log{Data: pack.Bytes()})
	require.Nil(t, ret)
}

func TestStateSnapshot(t *testing.T) {
	s, err := NewPartitionState()
	state := s.(*PartitionState)
	require.Nil(t, err)
	require.NotNil(t, state)
	for i := 0; i < 10; i++ {
		applyPartition(t, state, uint32(i))
	}

	fsmSnapshot, err := state.Snapshot()

	require.NoError(t, err)

	sink := mocks.NewSnapshotSink(t)
	sink.On("Write", mock.Anything).Return(0, nil)
	sink.On("Close").Return(nil)
	err = fsmSnapshot.Persist(sink)

	require.NoError(t, err)
}
