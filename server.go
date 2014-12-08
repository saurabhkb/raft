package main

import (
	"raft/log"
	"raft/util"
	"raft/storage"
)

const (
	FOLLOWER = "FOLLOWER"
	CANDIDATE = "CANDIDATE"
	LEADER = "LEADER"
)

// me
type Server struct {
	// identity
	Name string
	Pid int
	HostAddress util.Endpoint

	// raft related
	Replicas []*Replica
	Majority int
	CurrentTerm int
	State string
	VotedFor bool
	Timeout int
	HeartbeatTimeout int
	Timer *ElectionTimer

	Sresponder *Responder
}


/*
* Voting functions
*/
// can vote if I haven't voted before and the message term is >= mine
func (s *Server) RespondToRequestVote(voteRequest RaftMessage) RaftMessage {
	// if stale term, reject it
	if s.CurrentTerm > voteRequest.Term {
		return CreateVoteResponse(s.CurrentTerm, s.Pid, false)
	}

	// if term is newer than ours, update our term and demote self to follower
	if s.CurrentTerm < voteRequest.Term {
		s.State = FOLLOWER
		s.CurrentTerm = voteRequest.Term
	}

	// if i haven't voted yet
	if !s.VotedFor {
		s.VotedFor = true
		return CreateVoteResponse(s.CurrentTerm, s.Pid, true)
	}

	return CreateVoteResponse(s.CurrentTerm, s.Pid, false)
}


/*
* Append Entry functions
*/
func (s *Server) RespondToAppendEntry(appendEntryRequest RaftMessage) RaftMessage {
	// if stale term, reject it
	if s.CurrentTerm > appendEntryRequest.Term {
		return CreateAppendEntriesResponse(s.CurrentTerm, s.Pid, false)
	}

	// if term is newer than ours, update our term and demote self to follower
	if appendEntryRequest.Term > s.CurrentTerm {
		s.CurrentTerm = appendEntryRequest.Term
	}
	s.State = FOLLOWER

	// reset timer
	s.Timer.Reset(s.Timeout)

	success := log.Truncate(appendEntryRequest.PrevLogIndex, appendEntryRequest.PrevLogTerm)
	if !success {
		return CreateAppendEntriesResponse(s.CurrentTerm, s.Pid, false)
	}

	for i := 0; i < len(appendEntryRequest.Entries); i++ {
		success = log.Append(appendEntryRequest.Entries[i])
		if !success {
			return CreateAppendEntriesResponse(s.CurrentTerm, s.Pid, false)
		}
	}

	success = log.SetCommitIndex(appendEntryRequest.LeaderCommit)
	if !success {
		return CreateAppendEntriesResponse(s.CurrentTerm, s.Pid, false)
	}

	return CreateAppendEntriesResponse(s.CurrentTerm, s.Pid, true)
}


/*
* Client Interface
*/
// will try to append the value to the log and commit
func (s *Server) Execute(value string) {
	if s.State != LEADER {
		return
	}

	// je suis le leader
	entry := log.Entry{s.CurrentTerm, value}
	prevLogIndex := log.Top()
	prevLogTerm := log.TopTerm()
	leaderCommit := log.CommitIndex()
	log.Append(entry)

	// send appendEntry messages to each replica
	response := make(chan RaftMessage)
	go func() {
		for _, r := range s.Replicas {
			appendEntryRequest := CreateAppendEntriesMessage(s.CurrentTerm, s.Pid, prevLogIndex, prevLogTerm, []log.Entry{entry}, leaderCommit)
			r.SendRaftMessage(appendEntryRequest, response)
		}
	}()

	for {
		select {
			case msg := <-response: {
				util.P_out("%v", msg)
			}
		}
	}
}

/*
*	Trigger election
*/
func (s *Server) InitiateElection() {
	s.State = CANDIDATE
	s.CurrentTerm++
	response := make(chan RaftMessage)
	go func() {
		for _, r := range s.Replicas {
			util.P_out("sending vote request to %v", r.HostAddress)
			voteRequest := CreateVoteRequestMessage(s.CurrentTerm, s.Pid, 0, 0)	//TODO
			r.SendRaftMessage(voteRequest, response)
		}
	}()

	s.VotedFor = true	// i voted for myself
	numVotes := 1
	for s.state == CANDIDATE {
		select {
			case msg := <-response: {
				util.P_out("initiate election: %v", msg)
				switch msg.Type {
					case VOTE_RES: {
						numVotes++
					}
				}
			}
		}
		if numVotes > s.Majority {
			util.P_out("Yahoo!")
			break
		}
	}
}


/*
*	Control functions
*/
func (s *Server) Init(Name string, Pid int, HostAddress util.Endpoint, ElectionTimeout int, endpoints []util.Endpoint) {
	s.Name = Name
	s.Pid = Pid
	s.HostAddress = HostAddress
	s.Timeout = ElectionTimeout
	s.HeartbeatTimeout = 2
	s.State = FOLLOWER

	s.Timer = &ElectionTimer{}
	s.Timer.Init()
	s.Sresponder = &Responder{}
	s.Sresponder.Init(s.HostAddress.RepTcpFormat())

	storage.Init("/tmp/raftdb/" + Name)
	log.Init(Name)

	for _, e := range endpoints {
		s.Replicas = append(s.Replicas, CreateReplica(e))
	}
	s.Majority = len(endpoints) / 2 + 1
}


func (s *Server) Start() {
	s.Timer.Reset(s.Timeout)
	s.Timer.Start()
	for {
		select {
			case <-s.Timer.TimeoutEvent: {
				util.P_out("time's up!")
				if s.State == FOLLOWER {
					s.InitiateElection()
				}
				s.Timer.Reset(s.Timeout)
				s.Timer.TimeoutAck <- true
			}
			case msg := <-s.Sresponder.ReceiveEvent: {
				util.P_out("received raft message!: %v", msg)
				var reply RaftMessage
				switch msg.Type {
					case APPENDENTRIES_REQ: {
						s.Timer.Reset(s.Timeout)
						reply = s.RespondToAppendEntry(msg)
					}
					case APPENDENTRIES_RES: {
						util.P_out("received appendEntry response!: %v", msg)
					}
					case VOTE_REQ: {
						s.Timer.Reset(s.Timeout)
						reply = s.RespondToRequestVote(msg)
					}
					case VOTE_RES: {
						util.P_out("received vote response!: %v", msg)
					}
				}
				s.Sresponder.SendChannel <- reply
			}
		}
	}
}
