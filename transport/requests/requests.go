package requests

import "github.com/hashicorp/raft"

//go:generate deep-copy -o RequestVoteRequest.deepcopy.go --pointer-receiver --type RequestVoteRequest .
type RequestVoteRequest struct {
	*raft.RequestVoteRequest
}

//go:generate deep-copy -o InstallSnapshotRequest.deepcopy.go --pointer-receiver --type InstallSnapshotRequest .
type InstallSnapshotRequest struct {
	*raft.InstallSnapshotRequest
}

//go:generate deep-copy -o AppendEntriesRequest.deepcopy.go --pointer-receiver --type AppendEntriesRequest .
type AppendEntriesRequest struct {
	*raft.AppendEntriesRequest
}

//go:generate deep-copy -o TimeoutNowRequest.deepcopy.go --pointer-receiver --type TimeoutNowRequest .
type TimeoutNowRequest struct {
	*raft.TimeoutNowRequest
}

//go:generate deep-copy -o RequestPreVoteRequest.deepcopy.go --pointer-receiver --type RequestPreVoteRequest .
type RequestPreVoteRequest struct {
	*raft.RequestPreVoteRequest
}
