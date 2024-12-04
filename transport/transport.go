package transport

import (
	"github.com/dhiaayachi/raft"
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

// Transport provides an interface for network transports
// to allow Raft to communicate with other nodes.
type Transport interface {
	raft.Transport
	raft.WithPreVote
	raft.WithClose
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
