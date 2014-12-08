package main

import (
	"raft/util"
)


// how i refer to my friends on the consensus group
type Replica struct {
	HostAddress util.Endpoint
	VoteResponded bool
	VoteGranted bool
	NextIndex int
	MatchIndex int
}

func CreateReplica(addr util.Endpoint) *Replica {
	r := &Replica{}
	r.HostAddress = addr
	r.VoteResponded = false
	r.VoteGranted = false
	r.NextIndex = 0
	r.MatchIndex = 0

	return r
}

func (r *Replica) SendAppendRequest(req *AppendEntriesMessage) {
}
