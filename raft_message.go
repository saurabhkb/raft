package main

import (
	"raft/log"
	"encoding/json"
)

const (
	APPENDENTRIES_REQ = "APPENDENTRIES_REQ"
	APPENDENTRIES_RES = "APPENDENTRIES_RES"

	VOTE_REQ = "VOTE_REQ"
	VOTE_RES = "VOTE_RES"
)

type RaftMessage struct {
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

func (r RaftMessage) ToJson() string {
	jsonStr, _ := json.Marshal(r)
	return string(jsonStr[:])
}

func FromJson(s string) RaftMessage {
	m := RaftMessage{}
	json.Unmarshal([]byte(s), &m)
	return m
}

func CreateAppendEntriesMessage(term int, pid int, prevLogIndex int, prevLogTerm int, entries []log.Entry, leaderCommit int) RaftMessage {
	message := RaftMessage{}
	message.Type = APPENDENTRIES_REQ
	message.Term = term
	message.FromPid = pid
	message.PrevLogIndex = prevLogIndex
	message.PrevLogTerm = prevLogTerm
	message.Entries = entries
	message.LeaderCommit = leaderCommit
	return message
}

func CreateAppendEntriesResponse(term int, pid int, success bool) RaftMessage {
	m := RaftMessage{}
	m.Type = APPENDENTRIES_RES
	m.Term = term
	m.FromPid = pid
	m.Success = success
	return m
}

func CreateVoteRequestMessage(term, pid, lastLogIndex, lastLogTerm int) RaftMessage {
	m := RaftMessage{}
	m.Type = VOTE_REQ
	m.Term = term
	m.FromPid = pid
	m.LastLogIndex = lastLogIndex
	m.LastLogTerm = lastLogTerm
	return m
}


func CreateVoteResponse(term int, pid int, voteGranted bool) RaftMessage {
	m := RaftMessage{}
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

