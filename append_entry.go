package main

import (
	"raft/log"
)

type AppendEntriesMessage struct {
	Term int
	FromPID int

	PrevLogIndex int
	PrevLogTerm int

	Entries []log.Entry

	LeaderCommit int
}

type AppendEntriesResponse struct {
	Term int
	FromPID int
	Success bool
}

