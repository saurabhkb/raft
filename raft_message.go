package main

import (
	"raft/log"
	"encoding/json"
	"fmt"
)

const (
	_ = iota
	RAFT_APPEND_REQ
	RAFT_APPEND_REP
	RAFT_VOTE_REQ
	RAFT_VOTE_REP
	RAFT_CLIENT_SIZE_REQ
	RAFT_CLIENT_SIZE_REPLY
	RAFT_CLIENT_VALUE_REQ
	RAFT_CLIENT_VALUE_REPLY
)

type RaftMessage struct {
	Id string
	Type int

	Term int
	FromPid int

	PrevLogIndex int
	PrevLogTerm int

	Entries []log.Entry

	LeaderCommit int

	Success bool

	LastLogIndex int
	LastLogTerm int

	VoteGranted bool

	Size int
	Nodes NodeMap

	// for the client
	Ivalue int
}

func (r RaftMessage) String() string {
	switch r.Type {
		case RAFT_APPEND_REQ: {
			s := fmt.Sprintf("RAFT_APPEND_REQ (%s): Term:%d, FromPid:%d, prevLogIndex:%d, prevLogTerm:%d, leaderCommit:%d, entries:%v", r.Id, r.Term, r.FromPid, r.PrevLogIndex, r.PrevLogTerm, r.LeaderCommit, r.Entries)
			return s
		}
		case RAFT_APPEND_REP: {
			s := fmt.Sprintf("RAFT_APPEND_REP (%s): Term:%d, FromPid:%d, Success:%v", r.Id, r.Term, r.FromPid, r.Success)
			return s
		}
		case RAFT_VOTE_REQ: {
			s := fmt.Sprintf("RAFT_VOTE_REQ (%s): Term:%d, FromPid:%d, LastLogIndex:%d, LastLogTerm:%d", r.Id, r.Term, r.FromPid, r.LastLogIndex, r.LastLogTerm)
			return s
		}
		case RAFT_VOTE_REP: {
			s := fmt.Sprintf("RAFT_VOTE_REP (%s): Term:%d, FromPid:%d, VoteGranted:%v", r.Id, r.Term, r.FromPid, r.VoteGranted)
			return s
		}
		case RAFT_CLIENT_SIZE_REQ: {
			s := fmt.Sprintf("RAFT_CLIENT_SIZE_REQ: ToSize:%d, Nodes:%v", r.Size, r.Nodes)
			return s
		}
		case RAFT_CLIENT_SIZE_REPLY: {
			s := fmt.Sprintf("RAFT_CLIENT_SIZE_REPLY: ToSize:%d, Nodes:%v", r.Size, r.Nodes)
			return s
		}
		case RAFT_CLIENT_VALUE_REQ: {
			s := fmt.Sprintf("RAFT_CLIENT_VALUE_REQ: Ivalue:%d, Success:%v", r.Ivalue, r.Success)
			return s
		}
		case RAFT_CLIENT_VALUE_REPLY: {
			s := fmt.Sprintf("RAFT_CLIENT_VALUE_REPLY: Ivalue:%d, Sucess:%v", r.Ivalue, r.Success)
			return s
		}
		default: {
			s := fmt.Sprintf("Unknown Message Type: |%v|", r.Type)
			return s
		}
	}
}

func (r RaftMessage) ToJson() string {
	jsonStr, _ := json.Marshal(r)
	return string(jsonStr[:])
}

func FromJson(s string) RaftMessage {
	m := RaftMessage{}
	json.Unmarshal([]byte(s), &m)
	return m
}

func CreateAppendEntriesMessage(id string, term int, pid int, prevLogIndex int, prevLogTerm int, entries []log.Entry, leaderCommit int) RaftMessage {
	message := RaftMessage{}
	message.Id = id
	message.Type = RAFT_APPEND_REQ
	message.Term = term
	message.FromPid = pid
	message.PrevLogIndex = prevLogIndex
	message.PrevLogTerm = prevLogTerm
	message.Entries = entries
	message.LeaderCommit = leaderCommit
	return message
}

func CreateAppendEntriesResponse(id string, term int, pid int, commitIndex int, success bool) RaftMessage {
	m := RaftMessage{}
	m.Id = id
	m.Type = RAFT_APPEND_REP
	m.Term = term
	m.FromPid = pid
	m.Success = success
	m.LeaderCommit = commitIndex
	return m
}

func CreateVoteRequestMessage(id string, term, pid, lastLogIndex, lastLogTerm int) RaftMessage {
	m := RaftMessage{}
	m.Id = id
	m.Type = RAFT_VOTE_REQ
	m.Term = term
	m.FromPid = pid
	m.LastLogIndex = lastLogIndex
	m.LastLogTerm = lastLogTerm
	return m
}


func CreateVoteResponse(id string, term int, pid int, voteGranted bool) RaftMessage {
	m := RaftMessage{}
	m.Id = id
	m.Type = RAFT_VOTE_REP
	m.Term = term
	m.FromPid = pid
	m.VoteGranted = voteGranted
	return m
}

func CreateClientSizeRequestMessage(id string, size int, nodeMap NodeMap) RaftMessage {
	m := RaftMessage{}
	m.Id = id
	m.Type = RAFT_CLIENT_SIZE_REQ
	m.Size = size
	m.Nodes = nodeMap
	return m
}

func CreateClientSizeResponse(id string, success bool) RaftMessage {
	m := RaftMessage{}
	m.Id = id
	m.Type = RAFT_CLIENT_SIZE_REPLY
	m.Success = success
	return m
}

func CreateClientValueResponse(id string, success bool) RaftMessage {
	m := RaftMessage{}
	m.Id = id
	m.Type = RAFT_CLIENT_VALUE_REPLY
	m.Success = success
	return m
}

func CreateDiffLeaderResponse(id string, leaderPid int) RaftMessage {
	m := RaftMessage{}
	m.Id = id
	m.Type = RAFT_CLIENT_VALUE_REPLY
	m.Success = false
	m.Ivalue = leaderPid
	return m
}

// type AppendEntriesMessage struct {
// 	Term int
// 	FromPid int
// 
// 	PrevLogIndex int
// 	PrevLogTerm int
// 
// 	Entries []log.Entry
// 
// 	LeaderCommit int
// }
// 
// type AppendEntriesResponse struct {
// 	Term int
// 	FromPid int
// 	Success bool
// }
// 
// 
// type RequestVoteMessage struct {
// 	Term int
// 	FromPid int
// 
// 	LastLogIndex int
// 	LastLogTerm int
// }
// 
// type RequestVoteResponse struct {
// 	Term int
// 	FromPid int
// 	VoteGranted bool
// }

