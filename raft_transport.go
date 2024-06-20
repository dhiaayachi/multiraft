package multiraft

import (
	"context"
	"fmt"
	"github.com/hashicorp/raft"
	"io"
	"strconv"
	"strings"
)

const separator = "::"

type TransportRaft interface {
	raft.Transport
	raft.WithPreVote
	raft.WithClose
}

type MuxTransport struct {
	raftTransport TransportRaft
	consumerCh    chan RPC
	cancel        context.CancelFunc
}

func (r *MuxTransport) Consumer() <-chan RPC {
	return r.consumerCh
}

func (r *MuxTransport) LocalAddr() raft.ServerAddress {
	return r.raftTransport.LocalAddr()
}

func commandPartition(command interface{}, partition uint64) interface{} {
	header := command.(raft.RPCHeader)
	header.ID = []byte(fmt.Sprintf("%s%s%d", header.ID, separator, partition))
	return header
}

func (r *MuxTransport) AppendEntriesPipeline(id raft.ServerID, target raft.ServerAddress, partition uint64) (raft.AppendPipeline, error) {
	// TODO: fix pipeline to be able to pass partition
	return r.raftTransport.AppendEntriesPipeline(id, target)
}

func (r *MuxTransport) AppendEntries(id raft.ServerID, target raft.ServerAddress, args *raft.AppendEntriesRequest, resp *raft.AppendEntriesResponse, partition uint64) error {
	command := commandPartition(args, partition)
	return r.raftTransport.AppendEntries(id, target, command.(*raft.AppendEntriesRequest), resp)
}

func (r *MuxTransport) RequestVote(id raft.ServerID, target raft.ServerAddress, args *raft.RequestVoteRequest, resp *raft.RequestVoteResponse, partition uint64) error {
	command := commandPartition(args, partition)
	return r.raftTransport.RequestVote(id, target, command.(*raft.RequestVoteRequest), resp)
}

func (r *MuxTransport) InstallSnapshot(id raft.ServerID, target raft.ServerAddress, args *raft.InstallSnapshotRequest, resp *raft.InstallSnapshotResponse, data io.Reader, partition uint64) error {
	command := commandPartition(args, partition)
	return r.raftTransport.InstallSnapshot(id, target, command.(*raft.InstallSnapshotRequest), resp, data)
}

func (r *MuxTransport) SetHeartbeatHandler(cb func(rpc raft.RPC)) {
	r.raftTransport.SetHeartbeatHandler(cb)
}

func (r *MuxTransport) TimeoutNow(id raft.ServerID, target raft.ServerAddress, args *raft.TimeoutNowRequest, resp *raft.TimeoutNowResponse, partition uint64) error {
	command := commandPartition(args, partition)
	return r.raftTransport.TimeoutNow(id, target, command.(*raft.TimeoutNowRequest), resp)
}

func (r *MuxTransport) RequestPreVote(id raft.ServerID, target raft.ServerAddress, args *raft.RequestPreVoteRequest, resp *raft.RequestPreVoteResponse, partition uint64) error {
	command := commandPartition(args, partition)
	return r.raftTransport.RequestPreVote(id, target, command.(*raft.RequestPreVoteRequest), resp)
}

func (r *MuxTransport) Close() error {
	r.cancel()
	return r.raftTransport.Close()
}

func (r *MuxTransport) transportConsumer(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case rpc := <-r.raftTransport.Consumer():
			header := rpc.Command.(*raft.RPCHeader)
			id := strings.SplitN(string(header.ID), separator, 2)
			if len(id) != 2 {
				//TODO: add log here
				continue
			}
			header.ID = []byte(id[0])
			partition, err := strconv.ParseUint(id[1], 10, 64)
			if err != nil {
				// TODO: add log here
				continue
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
	raftTransport := MuxTransport{raftTransport: transport, consumerCh: make(chan RPC), cancel: cancel}
	go raftTransport.transportConsumer(ctx)
	return &raftTransport
}
