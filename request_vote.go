package main


type RequestVoteMessage struct {
	Term int
	FromPID int

	LastLogIndex int
	LastLogTerm int
}

type RequestVoteResponse struct {
	Term int
	FromPID int
	VoteGranted bool
}

