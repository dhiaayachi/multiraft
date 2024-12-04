package store

import (
	"fmt"
	"github.com/dhiaayachi/multiraft/encoding"
	"github.com/dhiaayachi/raft"
	"github.com/hashicorp/go-hclog"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestFsm_inServers(t *testing.T) {
	type fields struct {
		fsm       raft.FSM
		logger    hclog.Logger
		id        raft.ServerID
		raftAdder MockRaftAdder
	}
	type args struct {
		servers []raft.Server
	}
	tests := []*struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		{name: "in server", fields: fields{fsm: &raft.MockFSM{}, id: "server3"}, args: args{servers: []raft.Server{{ID: "server1"}, {ID: "server3"}}}, want: true},
		{name: "not in server", fields: fields{fsm: &raft.MockFSM{}, id: "server0"}, args: args{servers: []raft.Server{{ID: "server1"}, {ID: "server3"}}}, want: false},
		{name: "empty", fields: fields{fsm: &raft.MockFSM{}, id: "server0"}, args: args{servers: []raft.Server{}}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &Fsm{
				fsm:            tt.fields.fsm,
				logger:         tt.fields.logger,
				id:             tt.fields.id,
				partitionAdder: NewMockRaftAdder(t),
			}
			if got := f.inServers(tt.args.servers); got != tt.want {
				t.Errorf("inServers() = %v, want %v", got, tt.want)
			}
		})
	}
}

// errorFuture is used to return a static error.
type errorFuture struct {
	err error
}

func (e errorFuture) Error() error {
	return e.err
}

func TestApplyCreateRaft(t *testing.T) {
	raftAdder := NewMockRaftAdder(t)
	raftAdder.On("NewPartition", mock.Anything).Return(nil)
	raftAdder.On("BootstrapCluster", mock.Anything, mock.Anything).Return(errorFuture{nil})
	fsm, err := NewFSM(raftAdder, hclog.Default(), "id33")
	require.NoError(t, err)
	encodedLog, err := encoding.EncodeMsgPack(PartitionConfiguration{PartitionID: "part44", Servers: []raft.Server{{ID: "id33"}, {ID: "server3"}}})
	require.NoError(t, err)
	require.Nil(t, fsm.Apply(&raft.Log{Data: encodedLog.Bytes()}))

}

func TestApplyCreateRaftAddError(t *testing.T) {
	raftAdder := NewMockRaftAdder(t)
	mockFSM := raft.MockFSM{}
	raftAdder.On("NewPartition", mock.Anything).Return(fmt.Errorf("invalid partition"))
	fsm, err := NewFSM(raftAdder, hclog.Default(), "id33")
	require.NoError(t, err)
	encodedLog, err := encoding.EncodeMsgPack(PartitionConfiguration{PartitionID: "part44", Servers: []raft.Server{{ID: "id33"}, {ID: "server3"}}})
	require.NoError(t, err)
	require.NotNil(t, fsm.Apply(&raft.Log{Data: encodedLog.Bytes()}))
	require.Len(t, mockFSM.Logs(), 0)

}

func TestApplyCreateMsgPackError(t *testing.T) {
	raftAdder := NewMockRaftAdder(t)
	mockFSM := raft.MockFSM{}
	fsm, err := NewFSM(raftAdder, hclog.Default(), "id33")
	require.NoError(t, err)
	encodedLog, err := encoding.EncodeMsgPack("hello")
	require.NoError(t, err)
	require.NotNil(t, fsm.Apply(&raft.Log{Data: encodedLog.Bytes()}))
	require.Len(t, mockFSM.Logs(), 0)

}
