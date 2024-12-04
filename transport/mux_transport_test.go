package transport

import (
	"bytes"
	"github.com/dhiaayachi/multiraft/partition"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestMuxTransportNew(t *testing.T) {
	mockTransportRaft := NewMockRaftTransport(t)
	mockTransportRaft.On("Close").Return(nil)
	mockTransportRaft.On("Consumer").Maybe().Return(nil)
	trans := NewMuxTransport(mockTransportRaft, hclog.Default())
	defer func() { _ = trans.Close() }()
	require.NotNil(t, trans)

}

func TestMuxTransportLocalAddr(t *testing.T) {
	mockTransportRaft := NewMockRaftTransport(t)
	mockTransportRaft.On("Close").Return(nil)
	mockTransportRaft.On("LocalAddr").Return(raft.ServerAddress("node-addr-1"))
	mockTransportRaft.On("Consumer").Maybe().Return(nil)
	trans := NewMuxTransport(mockTransportRaft, hclog.Default())
	defer func() { _ = trans.Close() }()
	require.NotNil(t, trans)
	require.Equal(t, trans.LocalAddr(), raft.ServerAddress("node-addr-1"))

}

func TestMuxTransportAppendEntries(t *testing.T) {
	mockTransportRaft := NewMockRaftTransport(t)
	mockTransportRaft.On("AppendEntries", raft.ServerID("node1"),
		raft.ServerAddress("node1-addr"),
		&raft.AppendEntriesRequest{
			RPCHeader: raft.RPCHeader{ID: []byte("node1"), Meta: map[string]interface{}{partitionKey: partition.Typ("part1")}},
		}, mock.Anything).Return(nil)
	mockTransportRaft.On("Close").Return(nil)
	mockTransportRaft.On("Consumer").Maybe().Return(nil)
	trans := NewMuxTransport(mockTransportRaft, hclog.Default()).NewPartition("part1")
	defer func() { _ = trans.Close() }()
	require.NotNil(t, trans)
	require.NoError(t, trans.AppendEntries("node1", "node1-addr",
		&raft.AppendEntriesRequest{RPCHeader: raft.RPCHeader{ID: []byte("node1")}}, &raft.AppendEntriesResponse{}))
	mockTransportRaft.AssertNumberOfCalls(t, "AppendEntries", 1)
}

func TestMuxTransportRequestVote(t *testing.T) {
	mockTransportRaft := NewMockRaftTransport(t)
	mockTransportRaft.On("Close").Return(nil)
	mockTransportRaft.On("Consumer").Maybe().Return(nil)
	mockTransportRaft.On("RequestVote", raft.ServerID("node1"), raft.ServerAddress("node1-addr"),
		&raft.RequestVoteRequest{
			RPCHeader: raft.RPCHeader{ID: []byte("node1"), Meta: map[string]interface{}{partitionKey: partition.Typ("part22")}},
		}, mock.Anything).Return(nil)
	trans := NewMuxTransport(mockTransportRaft, hclog.Default()).NewPartition("part22")
	defer func() { _ = trans.Close() }()
	require.NotNil(t, trans)
	require.NoError(t, trans.RequestVote("node1", "node1-addr", &raft.RequestVoteRequest{RPCHeader: raft.RPCHeader{ID: []byte("node1")}}, &raft.RequestVoteResponse{}))
	mockTransportRaft.AssertNumberOfCalls(t, "RequestVote", 1)
}

func TestMuxTransportInstallSnapshot(t *testing.T) {
	mockTransportRaft := NewMockRaftTransport(t)
	mockTransportRaft.On("Close").Return(nil)
	mockTransportRaft.On("Consumer").Maybe().Return(nil)
	mockTransportRaft.On("InstallSnapshot", raft.ServerID("node1"), raft.ServerAddress("node1-addr"),
		&raft.InstallSnapshotRequest{
			RPCHeader: raft.RPCHeader{ID: []byte("node1"), Meta: map[string]interface{}{partitionKey: partition.Typ("part33")}},
		}, mock.Anything, mock.Anything).Return(nil)
	trans := NewMuxTransport(mockTransportRaft, hclog.Default()).NewPartition("part33")
	defer func() { _ = trans.Close() }()
	require.NotNil(t, trans)
	require.NoError(t, trans.InstallSnapshot("node1", "node1-addr", &raft.InstallSnapshotRequest{RPCHeader: raft.RPCHeader{ID: []byte("node1")}}, &raft.InstallSnapshotResponse{}, &bytes.Buffer{}))
	mockTransportRaft.AssertNumberOfCalls(t, "InstallSnapshot", 1)
}

func TestMuxTransportTimeoutNow(t *testing.T) {
	mockTransportRaft := NewMockRaftTransport(t)
	mockTransportRaft.On("Close").Return(nil)
	mockTransportRaft.On("Consumer").Maybe().Return(nil)
	mockTransportRaft.On("TimeoutNow", raft.ServerID("node1"), raft.ServerAddress("node1-addr"), &raft.TimeoutNowRequest{
		RPCHeader: raft.RPCHeader{ID: []byte("node1"), Meta: map[string]interface{}{partitionKey: partition.Typ("part32")}},
	}, mock.Anything).Return(nil)
	trans := NewMuxTransport(mockTransportRaft, hclog.Default()).NewPartition("part32")
	defer func() { _ = trans.Close() }()
	require.NotNil(t, trans)
	require.NoError(t, trans.TimeoutNow("node1", "node1-addr", &raft.TimeoutNowRequest{RPCHeader: raft.RPCHeader{ID: []byte("node1")}}, &raft.TimeoutNowResponse{}))
	mockTransportRaft.AssertNumberOfCalls(t, "TimeoutNow", 1)
}

func TestMuxTransportRequestPreVote(t *testing.T) {
	mockTransportRaft := NewMockRaftTransport(t)
	mockTransportRaft.On("Close").Return(nil)
	mockTransportRaft.On("Consumer").Maybe().Return(nil)
	mockTransportRaft.On("RequestPreVote", raft.ServerID("node1"), raft.ServerAddress("node1-addr"),
		&raft.RequestPreVoteRequest{
			RPCHeader: raft.RPCHeader{ID: []byte("node1"), Meta: map[string]interface{}{partitionKey: partition.Typ("part21")}},
		}, mock.Anything).Return(nil)
	trans := NewMuxTransport(mockTransportRaft, hclog.Default()).NewPartition("part21")
	defer func() { _ = trans.Close() }()
	require.NotNil(t, trans)
	require.NoError(t, trans.RequestPreVote("node1", "node1-addr", &raft.RequestPreVoteRequest{RPCHeader: raft.RPCHeader{ID: []byte("node1")}}, &raft.RequestPreVoteResponse{}))
	mockTransportRaft.AssertNumberOfCalls(t, "RequestPreVote", 1)
}

func TestMuxTransportSetHeartbeatHandler(t *testing.T) {
	mockTransportRaft := NewMockRaftTransport(t)
	mockTransportRaft.On("Close").Return(nil)
	mockTransportRaft.On("Consumer").Maybe().Return(nil)
	mockTransportRaft.On("SetHeartbeatHandler", mock.Anything).Return(nil)
	trans := NewMuxTransport(mockTransportRaft, hclog.Default())
	defer func() { _ = trans.Close() }()
	require.NotNil(t, trans)
	trans.SetHeartbeatHandler(func(rpc raft.RPC) {})
	mockTransportRaft.AssertNumberOfCalls(t, "SetHeartbeatHandler", 1)
}

func TestMuxTransportConsumer(t *testing.T) {
	ch := make(chan raft.RPC)
	mockTransportRaft := NewMockRaftTransport(t)
	mockTransportRaft.On("Close").Return(nil)
	mockTransportRaft.On("Consumer").Maybe().Return(ch)
	trans := NewMuxTransport(mockTransportRaft, hclog.Default()).NewPartition("part5")
	defer func() { _ = trans.Close() }()
	require.NotNil(t, trans)
	mch := trans.Consumer()
	ch <- raft.RPC{Command: &raft.RequestPreVoteRequest{RPCHeader: raft.RPCHeader{ID: []byte("node1"), Meta: map[string]interface{}{partitionKey: partition.Typ("part5")}}}}
	rpc := <-mch

	require.NotNil(t, rpc)
	require.Equal(t, rpc.Command.(raft.WithRPCHeader).GetRPCHeader().ID, []byte("node1"))
	require.Equal(t, rpc.Command.(raft.WithRPCHeader).GetRPCHeader().Meta[partitionKey], partition.Typ("part5"))

	ch <- raft.RPC{
		Command: &raft.AppendEntriesRequest{
			RPCHeader: raft.RPCHeader{
				ID:   []byte("node1"),
				Meta: map[string]interface{}{partitionKey: partition.Typ("part5")}},
		},
	}
	rpc = <-mch

	require.NotNil(t, rpc)
	require.Equal(t, rpc.Command.(raft.WithRPCHeader).GetRPCHeader().ID, []byte("node1"))
	require.Equal(t, rpc.Command.(raft.WithRPCHeader).GetRPCHeader().Meta[partitionKey], partition.Typ("part5"))

	ch <- raft.RPC{
		Command: &raft.RequestVoteRequest{
			RPCHeader: raft.RPCHeader{
				ID:   []byte("node1"),
				Meta: map[string]interface{}{partitionKey: partition.Typ("part5")}},
		},
	}
	rpc = <-mch

	require.NotNil(t, rpc)
	require.Equal(t, rpc.Command.(raft.WithRPCHeader).GetRPCHeader().ID, []byte("node1"))
	require.Equal(t, rpc.Command.(raft.WithRPCHeader).GetRPCHeader().Meta[partitionKey], partition.Typ("part5"))

	ch <- raft.RPC{
		Command: &raft.TimeoutNowRequest{
			RPCHeader: raft.RPCHeader{ID: []byte("node1"), Meta: map[string]interface{}{partitionKey: partition.Typ("part5")}},
		},
	}
	rpc = <-mch

	require.NotNil(t, rpc)
	require.Equal(t, rpc.Command.(raft.WithRPCHeader).GetRPCHeader().ID, []byte("node1"))
	require.Equal(t, rpc.Command.(raft.WithRPCHeader).GetRPCHeader().Meta[partitionKey], partition.Typ("part5"))

	ch <- raft.RPC{
		Command: &raft.InstallSnapshotRequest{
			RPCHeader: raft.RPCHeader{
				ID:   []byte("node1"),
				Meta: map[string]interface{}{partitionKey: partition.Typ("part5")}},
		},
	}
	rpc = <-mch

	require.NotNil(t, rpc)
	require.Equal(t, rpc.Command.(raft.WithRPCHeader).GetRPCHeader().ID, []byte("node1"))
	require.Equal(t, rpc.Command.(raft.WithRPCHeader).GetRPCHeader().Meta[partitionKey], partition.Typ("part5"))

	// no partition
	ch <- raft.RPC{Command: &raft.InstallSnapshotRequest{RPCHeader: raft.RPCHeader{ID: []byte("node1")}}}

	timer := time.After(50 * time.Millisecond)

	select {
	case <-timer:
	case <-mch:
		t.Fatal("no rpc should happen")

	}

	// invalid partition
	ch <- raft.RPC{
		Command: &raft.InstallSnapshotRequest{
			RPCHeader: raft.RPCHeader{
				ID:   []byte("node1"),
				Meta: map[string]interface{}{partitionKey: uint32(5)}},
		},
	}

	timer = time.After(50 * time.Millisecond)

	select {
	case <-timer:
	case <-mch:
		t.Fatal("no rpc should happen")

	}
}

func TestWithInMemTransport(t *testing.T) {
	addr := make([]raft.ServerAddress, 2)
	rTrans := make([]*raft.InmemTransport, 2)
	mTrans := make([]Transport, 2)
	addr[0], rTrans[0] = raft.NewInmemTransport("")
	addr[1], rTrans[1] = raft.NewInmemTransport("")

	for i := 0; i < 2; i++ {
		mTrans[i] = NewMuxTransport(rTrans[i], hclog.Default()).NewPartition("part9")
	}

	ch1 := mTrans[1].Consumer()

	rTrans[0].Connect(addr[1], rTrans[1])
	var rsp raft.RPC
	go func() {
		rsp = <-ch1
		rsp.RespChan <- raft.RPCResponse{Response: &raft.RequestVoteResponse{}}

	}()

	require.NoError(t, mTrans[0].RequestVote("id0", addr[1], &raft.RequestVoteRequest{}, &raft.RequestVoteResponse{}))
	require.NotNil(t, rsp)
	require.Equal(t, partition.Typ("part9"), rsp.Command.(*raft.RequestVoteRequest).GetRPCHeader().Meta[partitionKey])
}
