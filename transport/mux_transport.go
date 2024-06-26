package transport

import (
	"context"
	"fmt"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"io"
	"strconv"
	"strings"
)

const separator = "::"

//go:generate mockery --name TransportRaft --inpackage
type TransportRaft interface {
	raft.Transport
	raft.WithPreVote
	raft.WithClose
}

type MuxTransport struct {
	raftTransport TransportRaft
	consumerCh    chan RPC
	cancel        context.CancelFunc
	// Used for our logging
	logger hclog.Logger
}

func (r *MuxTransport) RaftTransport(_ uint32) raft.Transport {
	return r.raftTransport
}

func (r *MuxTransport) Consumer() <-chan RPC {
	return r.consumerCh
}

func (r *MuxTransport) LocalAddr() raft.ServerAddress {
	return r.raftTransport.LocalAddr()
}

func (r *MuxTransport) AppendEntriesPipeline(id raft.ServerID, target raft.ServerAddress, partition uint64) (raft.AppendPipeline, error) {
	// TODO: fix pipeline to be able to pass partition
	return r.raftTransport.AppendEntriesPipeline(id, target)
}

func (r *MuxTransport) AppendEntries(id raft.ServerID, target raft.ServerAddress, args *raft.AppendEntriesRequest, resp *raft.AppendEntriesResponse, partition uint64) error {
	args.RPCHeader.ID = []byte(fmt.Sprintf("%s%s%d", args.RPCHeader.ID, separator, partition))
	return r.raftTransport.AppendEntries(id, target, args, resp)
}

func (r *MuxTransport) RequestVote(id raft.ServerID, target raft.ServerAddress, args *raft.RequestVoteRequest, resp *raft.RequestVoteResponse, partition uint64) error {
	args.RPCHeader.ID = []byte(fmt.Sprintf("%s%s%d", args.RPCHeader.ID, separator, partition))
	return r.raftTransport.RequestVote(id, target, args, resp)
}

func (r *MuxTransport) InstallSnapshot(id raft.ServerID, target raft.ServerAddress, args *raft.InstallSnapshotRequest, resp *raft.InstallSnapshotResponse, data io.Reader, partition uint64) error {
	args.RPCHeader.ID = []byte(fmt.Sprintf("%s%s%d", args.RPCHeader.ID, separator, partition))
	return r.raftTransport.InstallSnapshot(id, target, args, resp, data)
}

func (r *MuxTransport) SetHeartbeatHandler(cb func(rpc raft.RPC)) {
	r.raftTransport.SetHeartbeatHandler(cb)
}

func (r *MuxTransport) TimeoutNow(id raft.ServerID, target raft.ServerAddress, args *raft.TimeoutNowRequest, resp *raft.TimeoutNowResponse, partition uint64) error {
	args.RPCHeader.ID = []byte(fmt.Sprintf("%s%s%d", args.RPCHeader.ID, separator, partition))
	return r.raftTransport.TimeoutNow(id, target, args, resp)
}

func (r *MuxTransport) RequestPreVote(id raft.ServerID, target raft.ServerAddress, args *raft.RequestPreVoteRequest, resp *raft.RequestPreVoteResponse, partition uint64) error {
	args.RPCHeader.ID = []byte(fmt.Sprintf("%s%s%d", args.RPCHeader.ID, separator, partition))
	return r.raftTransport.RequestPreVote(id, target, args, resp)
}

func (r *MuxTransport) Close() error {
	r.cancel()
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
			id := strings.SplitN(string(header.ID), separator, 2)
			if len(id) != 2 {
				r.logger.Error("invalid rpc command no partition detected", "id", id)
				continue
			}
			header.ID = []byte(id[0])
			partition, err := strconv.ParseUint(id[1], 10, 64)
			if err != nil {
				r.logger.Error("not able to parse partition number", "id", id, "partition", partition)
				continue
			}
			switch rpc.Command.(type) {
			case *raft.AppendEntriesRequest:
				cmd := rpc.Command.(*raft.AppendEntriesRequest)
				cmd.RPCHeader.ID = []byte(id[0])
			case *raft.RequestPreVoteRequest:
				cmd := rpc.Command.(*raft.RequestPreVoteRequest)
				cmd.RPCHeader.ID = []byte(id[0])
			case *raft.RequestVoteRequest:
				cmd := rpc.Command.(*raft.RequestVoteRequest)
				cmd.RPCHeader.ID = []byte(id[0])
			case *raft.TimeoutNowRequest:
				cmd := rpc.Command.(*raft.TimeoutNowRequest)
				cmd.RPCHeader.ID = []byte(id[0])
			case *raft.InstallSnapshotRequest:
				cmd := rpc.Command.(*raft.InstallSnapshotRequest)
				cmd.RPCHeader.ID = []byte(id[0])
			}
			newRPC := RPC{
				Command:      rpc.Command,
				partitionIdx: partition,
				Reader:       rpc.Reader,
				RespChan:     rpc.RespChan,
			}
			r.consumerCh <- newRPC
		}

	}
}

func NewMuxTransport(transport TransportRaft) Transport {
	ctx, cancel := context.WithCancel(context.Background())
	//TODO: fix logger
	raftTransport := MuxTransport{raftTransport: transport, consumerCh: make(chan RPC), cancel: cancel, logger: hclog.Default()}
	go raftTransport.transportConsumer(ctx)
	return &raftTransport
}
