package main

import (
	"raft/log"
	"raft/util"
	"raft/storage"
	"sync"
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

	state string
	Lock *sync.Mutex

	VotedFor bool
	Timeout int
	HeartbeatTimeout int
	Timer *ElectionTimer

	Sresponder *Responder

	ClientInterface chan string
	ClientAck chan bool
}

/*
* Atomic getters and setters
*/
func (s *Server) State() string {
	s.Lock.Lock()
	defer s.Lock.Unlock()
	return s.state
}

func (s *Server) SetState(st string) {
	s.Lock.Lock()
	defer s.Lock.Unlock()
	s.state = st
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
		s.SetState(FOLLOWER)
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
		util.P_out("stale term: %d > %d", s.CurrentTerm, appendEntryRequest.Term)
		return CreateAppendEntriesResponse(s.CurrentTerm, s.Pid, false)
	}

	// if term is newer than ours, update our term and demote self to follower
	if appendEntryRequest.Term > s.CurrentTerm {
		s.CurrentTerm = appendEntryRequest.Term
	}

	s.Timer.Reset(s.Timeout)
	s.SetState(FOLLOWER)

	// reset timer
	s.Timer.Reset(s.Timeout)

	success := log.Truncate(appendEntryRequest.PrevLogIndex, appendEntryRequest.PrevLogTerm)
	if !success {
		util.P_out("cannot truncate properly")
		return CreateAppendEntriesResponse(s.CurrentTerm, s.Pid, false)
	}

	for i := 0; i < len(appendEntryRequest.Entries); i++ {
		success = log.Append(appendEntryRequest.Entries[i])
		if !success {
		util.P_out("cannot append properly")
			return CreateAppendEntriesResponse(s.CurrentTerm, s.Pid, false)
		}
	}

	success = log.SetCommitIndex(appendEntryRequest.LeaderCommit)
	if !success {
		util.P_out("")
		return CreateAppendEntriesResponse(s.CurrentTerm, s.Pid, false)
	}

	util.P_out("%s", log.Stats())
	return CreateAppendEntriesResponse(s.CurrentTerm, s.Pid, true)
}

func (s *Server) RespondToAppendEntryResponse(appendEntryResponse RaftMessage) {
	// if term is newer than ours, update our term and demote self to follower
	if appendEntryResponse.Term > s.CurrentTerm {
		s.CurrentTerm = appendEntryResponse.Term
		s.SetState(FOLLOWER)
		s.Timer.Reset(s.Timeout)
	} else {
		if appendEntryResponse.Success {
			r.NextIndex++
		} else {
			r.NextIndex--
		}
	}
}


/*
* Client Interface
*/
// will try to append the value to the log and commit
func (s *Server) Execute(value string) {
	if s.State() != LEADER {
		return
	}

	// je suis le leader
	entry := log.Entry{s.CurrentTerm, value}
	// prevLogIndex := log.Top()
	// prevLogTerm := log.TopTerm()
	leaderCommit := log.CommitIndex()
	log.Append(entry)

	// send appendEntry messages to each replica
	response := make(chan RaftMessage)
	go func() {
		for _, r := range s.Replicas {
			prevLogIndex := r.NextIndex - 1
			prevLogTerm := log.GetTermFor(prevLogIndex)
			appendEntryRequest := CreateAppendEntriesMessage(s.CurrentTerm, s.Pid, prevLogIndex, prevLogTerm, []log.Entry{entry}, leaderCommit)
			r.SendRaftMessage(appendEntryRequest, response)
		}
	}()

	for {
		select {
			case msg := <-response: {
				util.P_out("%v", msg)
				s.RespondToAppendEntryResponse(msg)
			}
			case <-s.Timer.TimeoutEvent: {
				util.P_out("heartbeat!")
				s.Timer.Reset(s.HeartbeatTimeout)

				go func() {
					for _, r := range s.Replicas {
						util.P_out("sending heartbeat to %v", r.HostAddress)
						prevLogIndex := log.Top()
						prevLogTerm := log.TopTerm()
						leaderCommit := log.CommitIndex()
						heartbeat := CreateAppendEntriesMessage(s.CurrentTerm, s.Pid, prevLogIndex, prevLogTerm, []log.Entry{}, leaderCommit)
						r.SendRaftMessage(heartbeat, response)
					}
				}()

				s.Timer.TimeoutAck <- true
			}
		}
	}
}

/*
*	Trigger election
*/
func (s *Server) CandidateStart() {
	s.Timer.Reset(s.Timeout)
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
	for s.State() == CANDIDATE {
		select {
			case msg := <-response: {
				util.P_out("initiate election: %v", msg)
				switch msg.Type {
					case VOTE_RES: {
						if msg.VoteGranted {
							numVotes++
						} else {
							if s.CurrentTerm < msg.Term {
								s.SetState(FOLLOWER)
								s.CurrentTerm = msg.Term
								break
							}
						}
					}
				}
			}
			case <-s.Timer.TimeoutEvent: {
				util.P_out("timeout!")
				s.Timer.Reset(s.Timeout)
			}
			case msg := <-s.ClientInterface: {
				util.P_out("Not the leader, returning... %s", msg)
				s.ClientAck <- false
			}
		}
		if numVotes > s.Majority {
			util.P_out("Yahoo!")
			s.SetState(LEADER)
			break
		}
	}
}



func (s *Server) LeaderStart() {
	// send a heartbeat TODO
	s.Timer.Reset(s.HeartbeatTimeout)
	response := make(chan RaftMessage)
	for s.State() == LEADER {
		select {
			case msg := <-response: {
				util.P_out("on response, received: %v", msg)
			}
			case <-s.Timer.TimeoutEvent: {
				util.P_out("heartbeat!")
				s.Timer.Reset(s.HeartbeatTimeout)

				go func() {
					for _, r := range s.Replicas {
						util.P_out("sending heartbeat to %v", r.HostAddress)
						prevLogIndex := log.Top()
						prevLogTerm := log.TopTerm()
						leaderCommit := log.CommitIndex()
						heartbeat := CreateAppendEntriesMessage(s.CurrentTerm, s.Pid, prevLogIndex, prevLogTerm, []log.Entry{}, leaderCommit)
						r.SendRaftMessage(heartbeat, response)
					}
				}()

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
			case val := <-s.ClientInterface: {
				util.P_out("I am the leader, going to do something... %s", val)
				s.Execute(val)
				s.ClientAck <- true
			}
		}
	}
}

func (s *Server) FollowerStart() {
	for s.State() == FOLLOWER {
		select {
			case <-s.Timer.TimeoutEvent: {
				util.P_out("time's up!")
				s.Timer.Reset(s.Timeout)
				s.Timer.TimeoutAck <- true
				s.SetState(CANDIDATE)
				break
			}
			case msg := <-s.Sresponder.ReceiveEvent: {
				util.P_out("received raft message!: %v", msg)
				var reply RaftMessage
				switch msg.Type {
					case APPENDENTRIES_REQ: {
						s.Timer.Reset(s.Timeout)
						reply = s.RespondToAppendEntry(msg)
					}
					case VOTE_REQ: {
						s.Timer.Reset(s.Timeout)
						reply = s.RespondToRequestVote(msg)
					}
				}
				s.Sresponder.SendChannel <- reply
			}
			case msg := <-s.ClientInterface: {
				util.P_out("Not the leader, returning... %s", msg)
				s.ClientAck <- false
			}
		}
	}
}

/*
*	Control functions
*/
func (s *Server) Init(Name string, Pid int, HostAddress util.Endpoint, ElectionTimeout int, endpoints []util.Endpoint, interf chan string, ack chan bool) {
	s.Name = Name
	s.Pid = Pid
	s.HostAddress = HostAddress
	s.Timeout = ElectionTimeout
	s.HeartbeatTimeout = 2
	s.Lock = &sync.Mutex{}
	s.SetState(FOLLOWER)

	s.Timer = &ElectionTimer{}
	s.Timer.Init()
	s.Sresponder = &Responder{}
	s.Sresponder.Init(s.HostAddress.RepTcpFormat())

	s.ClientInterface = interf	// used to send commands to the server
	s.ClientAck = ack

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
		switch s.State() {
			case FOLLOWER: {
				s.FollowerStart()
			}
			case CANDIDATE: {
				s.CandidateStart()
			}
			case LEADER: {
				s.LeaderStart()
			}
		}
	}
}
