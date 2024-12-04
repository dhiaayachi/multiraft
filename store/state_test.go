package store

import (
	"bytes"
	"fmt"
	"github.com/dhiaayachi/multiraft/encoding"
	"github.com/dhiaayachi/multiraft/partition"
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

	require.Equal(t, c.PartitionID, partition.Typ("part1"))

	require.Nil(t, iter.Next())

	// Read all the entries
	iter, err = state.db.Txn(false).Get(partitionTable, indexID)

	require.NoError(t, err)

	c = iter.Next().(*PartitionConfiguration)
	require.NotNil(t, c)
	require.Equal(t, c.PartitionID, partition.Typ("part1"))

	c = iter.Next().(*PartitionConfiguration)
	require.NotNil(t, c)
	require.Equal(t, c.PartitionID, partition.Typ("part2"))

	require.Nil(t, iter.Next())
}

func applyPartition(t *testing.T, state *PartitionState, id partition.Typ) {
	pack, err := encoding.EncodeMsgPack(PartitionConfiguration{
		PartitionID: id,
	})
	require.Nil(t, err)
	ret := state.Apply(&raft.Log{Data: pack.Bytes()})
	require.Nil(t, ret)
}

func TestApplyPartitionInvalidMsgPack(t *testing.T) {
	// Create a state
	s, err := NewPartitionState()
	require.NoError(t, err)
	state := s.(*PartitionState)
	pack := "invalid msg"
	ret := state.Apply(&raft.Log{Data: []byte(pack)})
	require.Error(t, ret.(error))
}

func TestStateSnapshot(t *testing.T) {
	s, err := NewPartitionState()
	state := s.(*PartitionState)
	require.Nil(t, err)
	require.NotNil(t, state)
	for i := 0; i < 10; i++ {
		applyPartition(t, state, partition.Typ(fmt.Sprintf("part%d", i)))
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
	var configs []*PartitionConfiguration
	for {
		i := iter.Next()
		if i == nil {
			break
		}
		configs = append(configs, i.(*PartitionConfiguration))
	}
	require.NoError(t, err)
	require.Len(t, configs, 10)

}

func TestStateSnapshotSinkErrorOnClose(t *testing.T) {
	s, err := NewPartitionState()
	state := s.(*PartitionState)
	require.Nil(t, err)
	require.NotNil(t, state)
	for i := 0; i < 10; i++ {
		applyPartition(t, state, partition.Typ(fmt.Sprintf("part%d", i)))
	}

	fsmSnapshot, err := state.Snapshot()

	require.NoError(t, err)

	sink := mocks.NewSnapshotSink(t)
	var b []byte
	sink.On("Write", mock.Anything).Return(0, nil).Run(func(args mock.Arguments) {
		b = append(b, args.Get(0).([]byte)...)

	})
	sink.On("Close").Return(fmt.Errorf("error close sink"))
	err = fsmSnapshot.Persist(sink)
	require.Errorf(t, err, "error close sink")

}

func TestStateSnapshotSinkErrorOnWrite(t *testing.T) {
	s, err := NewPartitionState()
	state := s.(*PartitionState)
	require.Nil(t, err)
	require.NotNil(t, state)
	for i := 0; i < 10; i++ {
		applyPartition(t, state, partition.Typ(fmt.Sprintf("part%d", i)))
	}

	fsmSnapshot, err := state.Snapshot()

	require.NoError(t, err)

	sink := mocks.NewSnapshotSink(t)
	sink.On("Write", mock.Anything).Return(0, fmt.Errorf("error sink"))
	sink.On("Cancel").Return(nil)
	err = fsmSnapshot.Persist(sink)
	require.Errorf(t, err, "error sink")

}
