package transport

import (
	"context"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"io"
)

const separator = "::"
const partitionKey = "partition"
const ZeroPartition = 0

//go:generate mockery --name TransportRaft --inpackage
type TransportRaft interface {
	raft.Transport
	raft.WithPreVote
	raft.WithClose
}

//go:generate deep-copy -o transport.deepcopy.go --pointer-receiver --type RaftRPCHeader .
type RaftRPCHeader struct {
	raft.RPCHeader
}

type MuxTransport struct {
	raftTransport TransportRaft
	consumerCh    map[uint32]chan raft.RPC
	cancel        context.CancelFunc
	// Used for our logging
	logger    hclog.Logger
	partition uint32
}

func (r *MuxTransport) NewPartition(partition uint32) Transport {
	r.consumerCh[partition] = make(chan raft.RPC)
	return &MuxTransport{raftTransport: r.raftTransport, partition: partition, consumerCh: r.consumerCh, cancel: r.cancel, logger: r.logger}
}

func (r *MuxTransport) Consumer() <-chan raft.RPC {
	return r.consumerCh[r.partition]
}

func (r *MuxTransport) AppendEntriesPipeline(_ raft.ServerID, _ raft.ServerAddress) (raft.AppendPipeline, error) {
	// TODO: fix pipeline to be able to pass partition
	return nil, raft.ErrPipelineReplicationNotSupported
}

func (r *MuxTransport) AppendEntries(id raft.ServerID, target raft.ServerAddress, args *raft.AppendEntriesRequest, resp *raft.AppendEntriesResponse) error {
	hw := &RaftRPCHeader{RPCHeader: args.RPCHeader}
	header := hw.DeepCopy()
	if header.Meta == nil {
		header.Meta = make(map[string]interface{})
	}

	header.Meta[partitionKey] = r.partition
	args.RPCHeader = header.RPCHeader
	return r.raftTransport.AppendEntries(id, target, args, resp)
}

func (r *MuxTransport) RequestVote(id raft.ServerID, target raft.ServerAddress, args *raft.RequestVoteRequest, resp *raft.RequestVoteResponse) error {
	hw := &RaftRPCHeader{RPCHeader: args.RPCHeader}
	header := hw.DeepCopy()
	if header.Meta == nil {
		header.Meta = make(map[string]interface{})
	}

	header.Meta[partitionKey] = r.partition
	args.RPCHeader = header.RPCHeader
	return r.raftTransport.RequestVote(id, target, args, resp)
}

func (r *MuxTransport) InstallSnapshot(id raft.ServerID, target raft.ServerAddress, args *raft.InstallSnapshotRequest, resp *raft.InstallSnapshotResponse, data io.Reader) error {
	hw := &RaftRPCHeader{RPCHeader: args.RPCHeader}
	header := hw.DeepCopy()
	if header.Meta == nil {
		header.Meta = make(map[string]interface{})
	}

	header.Meta[partitionKey] = r.partition
	args.RPCHeader = header.RPCHeader
	return r.raftTransport.InstallSnapshot(id, target, args, resp, data)
}

func (r *MuxTransport) EncodePeer(id raft.ServerID, addr raft.ServerAddress) []byte {
	return r.raftTransport.EncodePeer(id, addr)
}

func (r *MuxTransport) DecodePeer(bytes []byte) raft.ServerAddress {
	return r.raftTransport.DecodePeer(bytes)
}

func (r *MuxTransport) TimeoutNow(id raft.ServerID, target raft.ServerAddress, args *raft.TimeoutNowRequest, resp *raft.TimeoutNowResponse) error {
	hw := &RaftRPCHeader{RPCHeader: args.RPCHeader}
	header := hw.DeepCopy()
	if header.Meta == nil {
		header.Meta = make(map[string]interface{})
	}

	header.Meta[partitionKey] = r.partition
	args.RPCHeader = header.RPCHeader
	return r.raftTransport.TimeoutNow(id, target, args, resp)
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

	hw := &RaftRPCHeader{RPCHeader: args.RPCHeader}
	header := hw.DeepCopy()
	if header.Meta == nil {
		header.Meta = make(map[string]interface{})
	}

	header.Meta[partitionKey] = r.partition
	args.RPCHeader = header.RPCHeader
	return r.raftTransport.RequestPreVote(id, target, args, resp)
}

func (r *MuxTransport) Close() error {
	r.cancel()
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
			p32 := partition.(uint32)
			if !ok {
				r.logger.Error("not able to parse meta for partition key")
				continue
			}

			newRPC := raft.RPC{
				Command:  rpc.Command,
				Reader:   rpc.Reader,
				RespChan: rpc.RespChan,
			}

			ch := r.consumerCh[p32]
			ch <- newRPC

		}

	}
}

func NewMuxTransport(transport TransportRaft) Transport {
	ctx, cancel := context.WithCancel(context.Background())
	//TODO: fix logger
	raftTransport := MuxTransport{raftTransport: transport, consumerCh: make(map[uint32]chan raft.RPC), cancel: cancel, logger: hclog.Default(), partition: ZeroPartition}
	go raftTransport.transportConsumer(ctx)
	return &raftTransport
}
