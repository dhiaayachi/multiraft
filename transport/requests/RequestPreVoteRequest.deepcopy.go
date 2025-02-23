// Code generated by deep-copy -o RequestPreVoteRequest.deepcopy.go --pointer-receiver --type RequestPreVoteRequest .; DO NOT EDIT.

package requests

import (
	"github.com/dhiaayachi/raft"
)

// DeepCopy generates a deep copy of *RequestPreVoteRequest
func (o *RequestPreVoteRequest) DeepCopy() *RequestPreVoteRequest {
	var cp RequestPreVoteRequest = *o
	if o.RequestPreVoteRequest != nil {
		cp.RequestPreVoteRequest = new(raft.RequestPreVoteRequest)
		*cp.RequestPreVoteRequest = *o.RequestPreVoteRequest
		if o.RequestPreVoteRequest.RPCHeader.ID != nil {
			cp.RequestPreVoteRequest.RPCHeader.ID = make([]byte, len(o.RequestPreVoteRequest.RPCHeader.ID))
			copy(cp.RequestPreVoteRequest.RPCHeader.ID, o.RequestPreVoteRequest.RPCHeader.ID)
		}
		if o.RequestPreVoteRequest.RPCHeader.Addr != nil {
			cp.RequestPreVoteRequest.RPCHeader.Addr = make([]byte, len(o.RequestPreVoteRequest.RPCHeader.Addr))
			copy(cp.RequestPreVoteRequest.RPCHeader.Addr, o.RequestPreVoteRequest.RPCHeader.Addr)
		}
		if o.RequestPreVoteRequest.RPCHeader.Meta != nil {
			cp.RequestPreVoteRequest.RPCHeader.Meta = make(map[string]any, len(o.RequestPreVoteRequest.RPCHeader.Meta))
			for k5, v5 := range o.RequestPreVoteRequest.RPCHeader.Meta {
				cp.RequestPreVoteRequest.RPCHeader.Meta[k5] = v5
			}
		}
	}
	return &cp
}
