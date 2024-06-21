package multiraft

import (
	"bytes"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestMuxTransportNew(t *testing.T) {
	mockTransportRaft := NewMockTransportRaft(t)
	mockTransportRaft.On("Close").Return(nil)
	mockTransportRaft.On("Consumer").Maybe().Return(nil)
	trans := NewMuxTransport(mockTransportRaft)
	defer trans.Close()
	require.NotNil(t, trans)
	require.Equal(t, trans.RaftTransport(26), mockTransportRaft)

}

func TestMuxTransportLocalAddr(t *testing.T) {
	mockTransportRaft := NewMockTransportRaft(t)
	mockTransportRaft.On("Close").Return(nil)
	mockTransportRaft.On("LocalAddr").Return(raft.ServerAddress("node-addr-1"))
	mockTransportRaft.On("Consumer").Maybe().Return(nil)
	trans := NewMuxTransport(mockTransportRaft)
	defer trans.Close()
	require.NotNil(t, trans)
	require.Equal(t, trans.LocalAddr(), raft.ServerAddress("node-addr-1"))

}

func TestMuxTransportAppendEntries(t *testing.T) {
	mockTransportRaft := NewMockTransportRaft(t)
	mockTransportRaft.On("AppendEntries", raft.ServerID("node1"), raft.ServerAddress("node1-addr"), &raft.AppendEntriesRequest{RPCHeader: raft.RPCHeader{ID: []byte("node1" + separator + "5")}}, mock.Anything).Return(nil)
	mockTransportRaft.On("Close").Return(nil)
	mockTransportRaft.On("Consumer").Maybe().Return(nil)
	trans := NewMuxTransport(mockTransportRaft)
	defer trans.Close()
	require.NotNil(t, trans)
	require.NoError(t, trans.AppendEntries("node1", "node1-addr", &raft.AppendEntriesRequest{RPCHeader: raft.RPCHeader{ID: []byte("node1")}}, &raft.AppendEntriesResponse{}, 5))
	mockTransportRaft.AssertNumberOfCalls(t, "AppendEntries", 1)
}

func TestMuxTransportRequestVote(t *testing.T) {
	mockTransportRaft := NewMockTransportRaft(t)
	mockTransportRaft.On("Close").Return(nil)
	mockTransportRaft.On("Consumer").Maybe().Return(nil)
	mockTransportRaft.On("RequestVote", raft.ServerID("node1"), raft.ServerAddress("node1-addr"), &raft.RequestVoteRequest{RPCHeader: raft.RPCHeader{ID: []byte("node1" + separator + "5")}}, mock.Anything).Return(nil)
	trans := NewMuxTransport(mockTransportRaft)
	defer trans.Close()
	require.NotNil(t, trans)
	require.NoError(t, trans.RequestVote("node1", "node1-addr", &raft.RequestVoteRequest{RPCHeader: raft.RPCHeader{ID: []byte("node1")}}, &raft.RequestVoteResponse{}, 5))
	mockTransportRaft.AssertNumberOfCalls(t, "RequestVote", 1)
}

func TestMuxTransportInstallSnapshot(t *testing.T) {
	mockTransportRaft := NewMockTransportRaft(t)
	mockTransportRaft.On("Close").Return(nil)
	mockTransportRaft.On("Consumer").Maybe().Return(nil)
	mockTransportRaft.On("InstallSnapshot", raft.ServerID("node1"), raft.ServerAddress("node1-addr"), &raft.InstallSnapshotRequest{RPCHeader: raft.RPCHeader{ID: []byte("node1" + separator + "5")}}, mock.Anything, mock.Anything).Return(nil)
	trans := NewMuxTransport(mockTransportRaft)
	defer trans.Close()
	require.NotNil(t, trans)
	require.NoError(t, trans.InstallSnapshot("node1", "node1-addr", &raft.InstallSnapshotRequest{RPCHeader: raft.RPCHeader{ID: []byte("node1")}}, &raft.InstallSnapshotResponse{}, &bytes.Buffer{}, 5))
	mockTransportRaft.AssertNumberOfCalls(t, "InstallSnapshot", 1)
}

func TestMuxTransportTimeoutNow(t *testing.T) {
	mockTransportRaft := NewMockTransportRaft(t)
	mockTransportRaft.On("Close").Return(nil)
	mockTransportRaft.On("Consumer").Maybe().Return(nil)
	mockTransportRaft.On("TimeoutNow", raft.ServerID("node1"), raft.ServerAddress("node1-addr"), &raft.TimeoutNowRequest{RPCHeader: raft.RPCHeader{ID: []byte("node1" + separator + "5")}}, mock.Anything).Return(nil)
	trans := NewMuxTransport(mockTransportRaft)
	defer trans.Close()
	require.NotNil(t, trans)
	require.NoError(t, trans.TimeoutNow("node1", "node1-addr", &raft.TimeoutNowRequest{RPCHeader: raft.RPCHeader{ID: []byte("node1")}}, &raft.TimeoutNowResponse{}, 5))
	mockTransportRaft.AssertNumberOfCalls(t, "TimeoutNow", 1)
}

func TestMuxTransportRequestPreVote(t *testing.T) {
	mockTransportRaft := NewMockTransportRaft(t)
	mockTransportRaft.On("Close").Return(nil)
	mockTransportRaft.On("Consumer").Maybe().Return(nil)
	mockTransportRaft.On("RequestPreVote", raft.ServerID("node1"), raft.ServerAddress("node1-addr"), &raft.RequestPreVoteRequest{RPCHeader: raft.RPCHeader{ID: []byte("node1" + separator + "5")}}, mock.Anything).Return(nil)
	trans := NewMuxTransport(mockTransportRaft)
	defer trans.Close()
	require.NotNil(t, trans)
	require.NoError(t, trans.RequestPreVote("node1", "node1-addr", &raft.RequestPreVoteRequest{RPCHeader: raft.RPCHeader{ID: []byte("node1")}}, &raft.RequestPreVoteResponse{}, 5))
	mockTransportRaft.AssertNumberOfCalls(t, "RequestPreVote", 1)
}

func TestMuxTransportSetHeartbeatHandler(t *testing.T) {
	mockTransportRaft := NewMockTransportRaft(t)
	mockTransportRaft.On("Close").Return(nil)
	mockTransportRaft.On("Consumer").Maybe().Return(nil)
	mockTransportRaft.On("SetHeartbeatHandler", mock.Anything).Return(nil)
	trans := NewMuxTransport(mockTransportRaft)
	defer trans.Close()
	require.NotNil(t, trans)
	trans.SetHeartbeatHandler(func(rpc raft.RPC) {})
	mockTransportRaft.AssertNumberOfCalls(t, "SetHeartbeatHandler", 1)
}

func TestMuxTransportConsumer(t *testing.T) {
	ch := make(chan raft.RPC)
	mockTransportRaft := NewMockTransportRaft(t)
	mockTransportRaft.On("Close").Return(nil)
	mockTransportRaft.On("Consumer").Maybe().Return(ch)
	trans := NewMuxTransport(mockTransportRaft)
	defer trans.Close()
	require.NotNil(t, trans)
	mch := trans.Consumer()
	ch <- raft.RPC{Command: &raft.RequestPreVoteRequest{RPCHeader: raft.RPCHeader{ID: []byte("node1" + separator + "5")}}}
	rpc := <-mch

	require.NotNil(t, rpc)
	require.Equal(t, rpc.Command.(raft.WithRPCHeader).GetRPCHeader().ID, []byte("node1"))
	require.Equal(t, rpc.partitionIdx, uint64(5))

	ch <- raft.RPC{Command: &raft.AppendEntriesRequest{RPCHeader: raft.RPCHeader{ID: []byte("node1" + separator + "5")}}}
	rpc = <-mch

	require.NotNil(t, rpc)
	require.Equal(t, rpc.Command.(raft.WithRPCHeader).GetRPCHeader().ID, []byte("node1"))
	require.Equal(t, rpc.partitionIdx, uint64(5))

	ch <- raft.RPC{Command: &raft.RequestVoteRequest{RPCHeader: raft.RPCHeader{ID: []byte("node1" + separator + "5")}}}
	rpc = <-mch

	require.NotNil(t, rpc)
	require.Equal(t, rpc.Command.(raft.WithRPCHeader).GetRPCHeader().ID, []byte("node1"))
	require.Equal(t, rpc.partitionIdx, uint64(5))

	ch <- raft.RPC{Command: &raft.TimeoutNowRequest{RPCHeader: raft.RPCHeader{ID: []byte("node1" + separator + "5")}}}
	rpc = <-mch

	require.NotNil(t, rpc)
	require.Equal(t, rpc.Command.(raft.WithRPCHeader).GetRPCHeader().ID, []byte("node1"))
	require.Equal(t, rpc.partitionIdx, uint64(5))

	ch <- raft.RPC{Command: &raft.InstallSnapshotRequest{RPCHeader: raft.RPCHeader{ID: []byte("node1" + separator + "5")}}}
	rpc = <-mch

	require.NotNil(t, rpc)
	require.Equal(t, rpc.Command.(raft.WithRPCHeader).GetRPCHeader().ID, []byte("node1"))
	require.Equal(t, rpc.partitionIdx, uint64(5))

	// no partition
	ch <- raft.RPC{Command: &raft.InstallSnapshotRequest{RPCHeader: raft.RPCHeader{ID: []byte("node1")}}}

	timer := time.After(50 * time.Millisecond)

	select {
	case <-timer:
	case <-mch:
		t.Fatal("no rpc should happen")

	}

	// invalid partition
	ch <- raft.RPC{Command: &raft.InstallSnapshotRequest{RPCHeader: raft.RPCHeader{ID: []byte("node1" + separator + "hello")}}}

	timer = time.After(50 * time.Millisecond)

	select {
	case <-timer:
	case <-mch:
		t.Fatal("no rpc should happen")

	}
}
