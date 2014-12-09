package main

import (
	"raft/log"
	"encoding/json"
	"fmt"
)

const (
	APPENDENTRIES_REQ = "APPENDENTRIES_REQ"
	APPENDENTRIES_RES = "APPENDENTRIES_RES"

	VOTE_REQ = "VOTE_REQ"
	VOTE_RES = "VOTE_RES"
)

type RaftMessage struct {
	Id string
	Type string

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

}

func (r RaftMessage) String() string {
	switch r.Type {
		case APPENDENTRIES_REQ: {
			s := fmt.Sprintf("APPENDENTRIES_REQ (%s): Term:%d, FromPid:%d, prevLogIndex:%d, prevLogTerm:%d, leaderCommit:%d, entries:%v", r.Id, r.Term, r.FromPid, r.PrevLogIndex, r.PrevLogTerm, r.LeaderCommit, r.Entries)
			return s
		}
		case APPENDENTRIES_RES: {
			s := fmt.Sprintf("APPENDENTRIES_RES (%s): Term:%d, FromPid:%d, Success:%v", r.Id, r.Term, r.FromPid, r.Success)
			return s
		}
		case VOTE_REQ: {
			s := fmt.Sprintf("VOTE_REQ (%s): Term:%d, FromPid:%d, LastLogIndex:%d, LastLogTerm:%d", r.Id, r.Term, r.FromPid, r.LastLogIndex, r.LastLogTerm)
			return s
		}
		case VOTE_RES: {
			s := fmt.Sprintf("VOTE_RES (%s): Term:%d, FromPid:%d, VoteGranted:%v", r.Id, r.Term, r.FromPid, r.VoteGranted)
			return s
		}
		default: {
			s := fmt.Sprintf("Unknown Message Type: %v", r.Type)
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
	message.Type = APPENDENTRIES_REQ
	message.Term = term
	message.FromPid = pid
	message.PrevLogIndex = prevLogIndex
	message.PrevLogTerm = prevLogTerm
	message.Entries = entries
	message.LeaderCommit = leaderCommit
	return message
}

func CreateAppendEntriesResponse(id string, term int, pid int, success bool) RaftMessage {
	m := RaftMessage{}
	m.Id = id
	m.Type = APPENDENTRIES_RES
	m.Term = term
	m.FromPid = pid
	m.Success = success
	return m
}

func CreateVoteRequestMessage(id string, term, pid, lastLogIndex, lastLogTerm int) RaftMessage {
	m := RaftMessage{}
	m.Id = id
	m.Type = VOTE_REQ
	m.Term = term
	m.FromPid = pid
	m.LastLogIndex = lastLogIndex
	m.LastLogTerm = lastLogTerm
	return m
}


func CreateVoteResponse(id string, term int, pid int, voteGranted bool) RaftMessage {
	m := RaftMessage{}
	m.Id = id
	m.Type = VOTE_RES
	m.Term = term
	m.FromPid = pid
	m.VoteGranted = voteGranted
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

