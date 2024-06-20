package multiraft

import (
	"github.com/hashicorp/raft"
	"io"
	"time"
)

// RPC has a command, and provides a response mechanism.
type RPC struct {
	partitionIdx uint64
	Command      interface{}
	Reader       io.Reader // Set only for InstallSnapshot
	RespChan     chan<- raft.RPCResponse
}

// Respond is used to respond with a response, error or both
func (r *RPC) Respond(resp interface{}, err error) {
	r.RespChan <- raft.RPCResponse{Response: resp, Error: err}
}

// Transport provides an interface for network transports
// to allow Raft to communicate with other nodes.
type Transport interface {
	// Consumer returns a channel that can be used to
	// consume and respond to RPC requests.
	Consumer() <-chan RPC

	// LocalAddr is used to return our local address to distinguish from our peers.
	LocalAddr() raft.ServerAddress

	// AppendEntriesPipeline returns an interface that can be used to pipeline
	// AppendEntries requests.
	AppendEntriesPipeline(id raft.ServerID, target raft.ServerAddress, partition uint64) (raft.AppendPipeline, error)

	// AppendEntries sends the appropriate RPC to the target node.
	AppendEntries(id raft.ServerID, target raft.ServerAddress, args *raft.AppendEntriesRequest, resp *raft.AppendEntriesResponse, partition uint64) error

	// RequestVote sends the appropriate RPC to the target node.
	RequestVote(id raft.ServerID, target raft.ServerAddress, args *raft.RequestVoteRequest, resp *raft.RequestVoteResponse, partition uint64) error

	// InstallSnapshot is used to push a snapshot down to a follower. The data is read from
	// the ReadCloser and streamed to the client.
	InstallSnapshot(id raft.ServerID, target raft.ServerAddress, args *raft.InstallSnapshotRequest, resp *raft.InstallSnapshotResponse, data io.Reader, partition uint64) error

	// SetHeartbeatHandler is used to setup a heartbeat handler
	// as a fast-pass. This is to avoid head-of-line blocking from
	// disk IO. If a Transport does not support this, it can simply
	// ignore the call, and push the heartbeat onto the Consumer channel.
	SetHeartbeatHandler(cb func(rpc raft.RPC))

	// TimeoutNow is used to start a leadership transfer to the target node.
	TimeoutNow(id raft.ServerID, target raft.ServerAddress, args *raft.TimeoutNowRequest, resp *raft.TimeoutNowResponse, partition uint64) error

	// RequestPreVote sends the appropriate RPC to the target node.
	RequestPreVote(id raft.ServerID, target raft.ServerAddress, args *raft.RequestPreVoteRequest, resp *raft.RequestPreVoteResponse, partition uint64) error

	// Close permanently closes a transport, stopping
	// any associated goroutines and freeing other resources.
	Close() error
	RaftTransport(partition uint64) raft.Transport
}

// AppendFuture is used to return information about a pipelined AppendEntries request.
type AppendFuture interface {
	raft.Future

	// Start returns the time that the append request was started.
	// It is always OK to call this method.
	Start() time.Time

	// Request holds the parameters of the AppendEntries call.
	// It is always OK to call this method.
	Request() *raft.AppendEntriesRequest

	// Response holds the results of the AppendEntries call.
	// This method must only be called after the Error
	// method returns, and will only be valid on success.
	Response() *raft.AppendEntriesResponse
}
