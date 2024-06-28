package store

import (
	"bytes"
	"fmt"
	"github.com/dhiaayachi/multiraft/consts"
	"github.com/dhiaayachi/multiraft/encoding"
	"github.com/dhiaayachi/multiraft/store/mocks"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"io"
	"testing"
)

func TestCreateStateAndApply(t *testing.T) {
	// Create a state
	s, err := NewPartitionState()
	state := s.(*PartitionState)
	require.Nil(t, err)
	require.NotNil(t, state)

	//Apply a log
	applyPartition(t, state, "part1")

	// Apply another log
	applyPartition(t, state, "part2")

	// Read the first entry and check it's ok
	iter, err := state.db.Txn(false).Get(partitionTable, indexID, "part1")

	require.NoError(t, err)

	c := iter.Next().(*PartitionConfiguration)
	require.NotNil(t, c)

	require.Equal(t, c.PartitionID, consts.PartitionType("part1"))

	require.Nil(t, iter.Next())

	// Read all the entries
	iter, err = state.db.Txn(false).Get(partitionTable, indexID)

	require.NoError(t, err)

	c = iter.Next().(*PartitionConfiguration)
	require.NotNil(t, c)
	require.Equal(t, c.PartitionID, consts.PartitionType("part1"))

	c = iter.Next().(*PartitionConfiguration)
	require.NotNil(t, c)
	require.Equal(t, c.PartitionID, consts.PartitionType("part2"))

	require.Nil(t, iter.Next())
}

func applyPartition(t *testing.T, state *PartitionState, id consts.PartitionType) {
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
		applyPartition(t, state, consts.PartitionType(fmt.Sprintf("part%d", i)))
	}

	fsmSnapshot, err := state.Snapshot()

	require.NoError(t, err)

	sink := mocks.NewSnapshotSink(t)
	var b []byte
	sink.On("Write", mock.Anything).Return(0, nil).Run(func(args mock.Arguments) {
		b = append(b, args.Get(0).([]byte)...)

	})
	sink.On("Close").Return(nil)
	err = fsmSnapshot.Persist(sink)

	require.NoError(t, err)

	err = s.Restore(io.NopCloser(bytes.NewBuffer(b)))
	require.NoError(t, err)
	s2, err := NewPartitionState()
	state2 := s2.(*PartitionState)
	require.Nil(t, err)
	require.NotNil(t, state2)
	iter, err := state.db.Txn(false).Get(partitionTable, indexID)
	require.NoError(t, err)
	require.NotNil(t, iter)
	var confs []*PartitionConfiguration
	for {
		i := iter.Next()
		if i == nil {
			break
		}
		confs = append(confs, i.(*PartitionConfiguration))
	}
	require.NoError(t, err)
	require.Len(t, confs, 10)

}
