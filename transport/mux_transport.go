package transport

import (
	"context"
	"github.com/dhiaayachi/multiraft/partition"
	"github.com/dhiaayachi/multiraft/transport/requests"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"io"
	"sync"
)

const partitionKey = "partition"

//go:generate mockery --name RaftTransport --inpackage
type RaftTransport interface {
	raft.Transport
	raft.WithPreVote
	raft.WithClose
}

type MuxTransport struct {
	raftTransport  RaftTransport
	consumerCh     map[partition.Typ]chan raft.RPC
	consumerChLock sync.RWMutex
	cancel         context.CancelFunc
	// Used for our logging
	logger   hclog.Logger
	zeroPart Transport
}

type PartitionTransport struct {
	raftTransport RaftTransport
	consumerCh    chan raft.RPC
	cancel        context.CancelFunc
	logger        hclog.Logger
	partition     partition.Typ
}

func (r *MuxTransport) NewPartition(partition partition.Typ) Transport {
	r.consumerChLock.Lock()
	defer r.consumerChLock.Unlock()
	r.consumerCh[partition] = make(chan raft.RPC)
	return &PartitionTransport{raftTransport: r.raftTransport, consumerCh: r.consumerCh[partition], cancel: r.cancel, logger: r.logger, partition: partition}
}

func (r *PartitionTransport) Consumer() <-chan raft.RPC {
	return r.consumerCh
}

func (r *PartitionTransport) AppendEntriesPipeline(_ raft.ServerID, _ raft.ServerAddress) (raft.AppendPipeline, error) {
	// TODO: fix pipeline to be able to pass partition
	return nil, raft.ErrPipelineReplicationNotSupported
}

func (r *PartitionTransport) AppendEntries(id raft.ServerID, target raft.ServerAddress, args *raft.AppendEntriesRequest, resp *raft.AppendEntriesResponse) error {
	argsW := &requests.AppendEntriesRequest{AppendEntriesRequest: args}
	argsCopy := argsW.DeepCopy()
	if argsCopy.Meta == nil {
		argsCopy.Meta = make(map[string]interface{})
	}

	argsCopy.Meta[partitionKey] = r.partition
	return r.raftTransport.AppendEntries(id, target, argsCopy.AppendEntriesRequest, resp)
}

func (r *PartitionTransport) RequestVote(id raft.ServerID, target raft.ServerAddress, args *raft.RequestVoteRequest, resp *raft.RequestVoteResponse) error {
	argsW := &requests.RequestVoteRequest{RequestVoteRequest: args}
	argsCopy := argsW.DeepCopy()
	if argsCopy.RequestVoteRequest.Meta == nil {
		argsCopy.RequestVoteRequest.Meta = make(map[string]interface{})
	}

	argsCopy.RequestVoteRequest.Meta[partitionKey] = r.partition
	return r.raftTransport.RequestVote(id, target, argsCopy.RequestVoteRequest, resp)
}

func (r *PartitionTransport) InstallSnapshot(id raft.ServerID, target raft.ServerAddress, args *raft.InstallSnapshotRequest, resp *raft.InstallSnapshotResponse, data io.Reader) error {
	argsW := &requests.InstallSnapshotRequest{InstallSnapshotRequest: args}
	argsCopy := argsW.DeepCopy()
	if argsCopy.Meta == nil {
		argsCopy.Meta = make(map[string]interface{})
	}

	argsCopy.Meta[partitionKey] = r.partition
	return r.raftTransport.InstallSnapshot(id, target, argsCopy.InstallSnapshotRequest, resp, data)
}

func (r *PartitionTransport) EncodePeer(id raft.ServerID, addr raft.ServerAddress) []byte {
	return r.raftTransport.EncodePeer(id, addr)
}

func (r *PartitionTransport) DecodePeer(bytes []byte) raft.ServerAddress {
	return r.raftTransport.DecodePeer(bytes)
}

func (r *PartitionTransport) TimeoutNow(id raft.ServerID, target raft.ServerAddress, args *raft.TimeoutNowRequest, resp *raft.TimeoutNowResponse) error {
	argsW := &requests.TimeoutNowRequest{TimeoutNowRequest: args}
	argsCopy := argsW.DeepCopy()
	if argsCopy.Meta == nil {
		argsCopy.Meta = make(map[string]interface{})
	}

	argsCopy.Meta[partitionKey] = r.partition
	return r.raftTransport.TimeoutNow(id, target, argsCopy.TimeoutNowRequest, resp)
}

func (r *PartitionTransport) RaftTransport(_ uint32) raft.Transport {
	return r.raftTransport
}

func (r *PartitionTransport) LocalAddr() raft.ServerAddress {
	return r.raftTransport.LocalAddr()
}

func (r *PartitionTransport) SetHeartbeatHandler(cb func(rpc raft.RPC)) {
	r.raftTransport.SetHeartbeatHandler(cb)
}

func (r *PartitionTransport) RequestPreVote(id raft.ServerID, target raft.ServerAddress, args *raft.RequestPreVoteRequest, resp *raft.RequestPreVoteResponse) error {
	argsW := &requests.RequestPreVoteRequest{RequestPreVoteRequest: args}
	argsCopy := argsW.DeepCopy()
	if argsCopy.Meta == nil {
		argsCopy.Meta = make(map[string]interface{})
	}

	argsCopy.Meta[partitionKey] = r.partition
	return r.raftTransport.RequestPreVote(id, target, argsCopy.RequestPreVoteRequest, resp)
}

func (r *PartitionTransport) Close() error {
	close(r.consumerCh)
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
			part, ok := header.Meta[partitionKey]

			if !ok {
				r.logger.Error("not able to parse meta for partition key")
				continue
			}
			partitionId, ok := part.(partition.Typ)
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
			ch := r.consumerCh[partitionId]
			r.consumerChLock.RUnlock()
			ch <- newRPC

		}

	}
}

func (r *MuxTransport) Close() error {
	return r.zeroPart.Close()
}

func (r *MuxTransport) LocalAddr() raft.ServerAddress {
	return r.zeroPart.LocalAddr()
}

func (r *MuxTransport) SetHeartbeatHandler(cb func(rpc raft.RPC)) {
	r.raftTransport.SetHeartbeatHandler(cb)
}

func NewMuxTransport(transport RaftTransport, logger hclog.Logger) *MuxTransport {
	ctx, cancel := context.WithCancel(context.Background())
	muxTransport := MuxTransport{raftTransport: transport, consumerCh: make(map[partition.Typ]chan raft.RPC), cancel: cancel, logger: logger}
	zeroPart := muxTransport.NewPartition(partition.Zero)
	muxTransport.zeroPart = zeroPart
	go muxTransport.transportConsumer(ctx)
	return &muxTransport
}
