package transport

import (
	"context"
	"github.com/dhiaayachi/multiraft/consts"
	"github.com/dhiaayachi/multiraft/transport/requests"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"io"
	"sync"
)

const separator = "::"
const partitionKey = "partition"

//go:generate mockery --name TransportRaft --inpackage
type TransportRaft interface {
	raft.Transport
	raft.WithPreVote
	raft.WithClose
}

type MuxTransport struct {
	raftTransport  TransportRaft
	consumerCh     map[consts.PartitionType]chan raft.RPC
	consumerChLock sync.RWMutex
	cancel         context.CancelFunc
	// Used for our logging
	logger    hclog.Logger
	partition consts.PartitionType
}

func (r *MuxTransport) NewPartition(partition consts.PartitionType) Transport {
	r.consumerChLock.Lock()
	defer r.consumerChLock.Unlock()
	r.consumerCh[partition] = make(chan raft.RPC)
	return &MuxTransport{raftTransport: r.raftTransport, partition: partition, consumerCh: r.consumerCh, cancel: r.cancel, logger: r.logger}
}

func (r *MuxTransport) Consumer() <-chan raft.RPC {
	r.consumerChLock.RLock()
	defer r.consumerChLock.RUnlock()
	return r.consumerCh[r.partition]
}

func (r *MuxTransport) AppendEntriesPipeline(_ raft.ServerID, _ raft.ServerAddress) (raft.AppendPipeline, error) {
	// TODO: fix pipeline to be able to pass partition
	return nil, raft.ErrPipelineReplicationNotSupported
}

func (r *MuxTransport) AppendEntries(id raft.ServerID, target raft.ServerAddress, args *raft.AppendEntriesRequest, resp *raft.AppendEntriesResponse) error {
	argsW := &requests.AppendEntriesRequest{AppendEntriesRequest: args}
	argsCopy := argsW.DeepCopy()
	if argsCopy.Meta == nil {
		argsCopy.Meta = make(map[string]interface{})
	}

	argsCopy.Meta[partitionKey] = r.partition
	return r.raftTransport.AppendEntries(id, target, argsCopy.AppendEntriesRequest, resp)
}

func (r *MuxTransport) RequestVote(id raft.ServerID, target raft.ServerAddress, args *raft.RequestVoteRequest, resp *raft.RequestVoteResponse) error {
	argsW := &requests.RequestVoteRequest{RequestVoteRequest: args}
	argsCopy := argsW.DeepCopy()
	if argsCopy.RequestVoteRequest.Meta == nil {
		argsCopy.RequestVoteRequest.Meta = make(map[string]interface{})
	}

	argsCopy.RequestVoteRequest.Meta[partitionKey] = r.partition
	return r.raftTransport.RequestVote(id, target, argsCopy.RequestVoteRequest, resp)
}

func (r *MuxTransport) InstallSnapshot(id raft.ServerID, target raft.ServerAddress, args *raft.InstallSnapshotRequest, resp *raft.InstallSnapshotResponse, data io.Reader) error {
	argsW := &requests.InstallSnapshotRequest{InstallSnapshotRequest: args}
	argsCopy := argsW.DeepCopy()
	if argsCopy.Meta == nil {
		argsCopy.Meta = make(map[string]interface{})
	}

	argsCopy.Meta[partitionKey] = r.partition
	return r.raftTransport.InstallSnapshot(id, target, argsCopy.InstallSnapshotRequest, resp, data)
}

func (r *MuxTransport) EncodePeer(id raft.ServerID, addr raft.ServerAddress) []byte {
	return r.raftTransport.EncodePeer(id, addr)
}

func (r *MuxTransport) DecodePeer(bytes []byte) raft.ServerAddress {
	return r.raftTransport.DecodePeer(bytes)
}

func (r *MuxTransport) TimeoutNow(id raft.ServerID, target raft.ServerAddress, args *raft.TimeoutNowRequest, resp *raft.TimeoutNowResponse) error {
	argsW := &requests.TimeoutNowRequest{TimeoutNowRequest: args}
	argsCopy := argsW.DeepCopy()
	if argsCopy.Meta == nil {
		argsCopy.Meta = make(map[string]interface{})
	}

	argsCopy.Meta[partitionKey] = r.partition
	return r.raftTransport.TimeoutNow(id, target, argsCopy.TimeoutNowRequest, resp)
}

func (r *MuxTransport) RaftTransport(_ uint32) raft.Transport {
	return r.raftTransport
}

func (r *MuxTransport) LocalAddr() raft.ServerAddress {
	return r.raftTransport.LocalAddr()
}

func (r *MuxTransport) SetHeartbeatHandler(cb func(rpc raft.RPC)) {
	r.raftTransport.SetHeartbeatHandler(cb)
}

func (r *MuxTransport) RequestPreVote(id raft.ServerID, target raft.ServerAddress, args *raft.RequestPreVoteRequest, resp *raft.RequestPreVoteResponse) error {
	argsW := &requests.RequestPreVoteRequest{RequestPreVoteRequest: args}
	argsCopy := argsW.DeepCopy()
	if argsCopy.Meta == nil {
		argsCopy.Meta = make(map[string]interface{})
	}

	argsCopy.Meta[partitionKey] = r.partition
	return r.raftTransport.RequestPreVote(id, target, argsCopy.RequestPreVoteRequest, resp)
}

func (r *MuxTransport) Close() error {
	r.cancel()
	r.consumerChLock.RLock()
	defer r.consumerChLock.RUnlock()
	for _, ch := range r.consumerCh {
		close(ch)

	}

	return r.raftTransport.Close()
}

func (r *MuxTransport) transportConsumer(ctx context.Context) {
	consumer := r.raftTransport.Consumer()
	for {
		select {
		case <-ctx.Done():
			return
		case rpc := <-consumer:
			header := rpc.Command.(raft.WithRPCHeader).GetRPCHeader()

			if header.Meta == nil {
				r.logger.Error("not able to parse meta for partition")
				continue
			}
			partition, ok := header.Meta[partitionKey]

			if !ok {
				r.logger.Error("not able to parse meta for partition key")
				continue
			}
			p32, ok := partition.(consts.PartitionType)
			if !ok {
				r.logger.Error("not able to parse meta for partition key (type)")
				continue
			}
			newRPC := raft.RPC{
				Command:  rpc.Command,
				Reader:   rpc.Reader,
				RespChan: rpc.RespChan,
			}

			r.consumerChLock.RLock()
			ch := r.consumerCh[p32]
			r.consumerChLock.RUnlock()
			ch <- newRPC

		}

	}
}

func NewMuxTransport(transport TransportRaft) Transport {
	ctx, cancel := context.WithCancel(context.Background())
	//TODO: fix logger
	raftTransport := MuxTransport{raftTransport: transport, consumerCh: make(map[consts.PartitionType]chan raft.RPC), cancel: cancel, logger: hclog.Default(), partition: consts.ZeroPartition}
	go raftTransport.transportConsumer(ctx)
	return &raftTransport
}
