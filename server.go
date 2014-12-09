package main

import (
	"raft/log"
	"raft/util"
	"raft/storage"
	"sync"
	"sort"
	"fmt"
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

	Syncs int

	messageCounter int
}

/*
* Message Id
*/
func (s *Server) MessageId() string {
	s.messageCounter++
	return fmt.Sprintf("%s:%d", s.Name, s.messageCounter)
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
		return CreateVoteResponse(voteRequest.Id, s.CurrentTerm, s.Pid, false)
	}

	// if term is newer than ours, update our term and demote self to follower
	if s.CurrentTerm < voteRequest.Term {
		s.SetState(FOLLOWER)
		s.CurrentTerm = voteRequest.Term
	}

	// if i haven't voted yet
	if !s.VotedFor {
		s.VotedFor = true
		return CreateVoteResponse(voteRequest.Id, s.CurrentTerm, s.Pid, true)
	}

	return CreateVoteResponse(voteRequest.Id, s.CurrentTerm, s.Pid, false)
}


/*
* Append Entry functions
*/
func (s *Server) RespondToAppendEntry(appendEntryRequest RaftMessage) RaftMessage {
	// if stale term, reject it
	if s.CurrentTerm > appendEntryRequest.Term {
		util.P_out("stale term: %d > %d", s.CurrentTerm, appendEntryRequest.Term)
		return CreateAppendEntriesResponse(appendEntryRequest.Id, s.CurrentTerm, s.Pid, log.CommitIndex(), false)
	}

	// if term is newer than ours, update our term and demote self to follower
	if appendEntryRequest.Term > s.CurrentTerm {
		s.CurrentTerm = appendEntryRequest.Term
	}

	s.Timer.Reset(s.Timeout)
	s.SetState(FOLLOWER)
	s.VotedFor = false

	// reset timer
	s.Timer.Reset(s.Timeout)

	success := log.Truncate(appendEntryRequest.PrevLogIndex, appendEntryRequest.PrevLogTerm)
	if !success {
		util.P_out("cannot truncate properly")
		return CreateAppendEntriesResponse(appendEntryRequest.Id, s.CurrentTerm, s.Pid, log.CommitIndex(), false)
	}

	for i := 0; i < len(appendEntryRequest.Entries); i++ {
		success = log.Append(appendEntryRequest.Entries[i])
		if !success {
		util.P_out("cannot append properly")
			return CreateAppendEntriesResponse(appendEntryRequest.Id, s.CurrentTerm, s.Pid, log.CommitIndex(), false)
		}
	}

	success = log.SetCommitIndex(appendEntryRequest.LeaderCommit)
	if !success {
		util.P_out("")
		return CreateAppendEntriesResponse(appendEntryRequest.Id, s.CurrentTerm, s.Pid, log.CommitIndex(), false)
	}

	util.P_out("%s", log.Stats())
	return CreateAppendEntriesResponse(appendEntryRequest.Id, s.CurrentTerm, s.Pid, log.CommitIndex(), true)
}

// TODO not sure about the bool
func (s *Server) RespondToAppendEntryResponse(appendEntryResponse RaftMessage) bool {
	// if term is newer than ours, update our term and demote self to follower
	if appendEntryResponse.Term > s.CurrentTerm {
		s.CurrentTerm = appendEntryResponse.Term
		s.SetState(FOLLOWER)
		s.Timer.Reset(s.Timeout)
		s.VotedFor = false
		return true
	} else {
		if appendEntryResponse.Success {
			s.Syncs++

			if s.Syncs < s.Majority {
				return false
			}

			var indices []int
			indices = append(indices, log.Top())
			for _, r := range s.Replicas {
				indices = append(indices, r.GetMatchIndex())
			}
			sort.Ints(indices)
			util.P_out("indices: %v", indices)

			c_idx := indices[s.Majority - 1]
			if c_idx > log.CommitIndex() {
				ret := log.SetCommitIndex(c_idx)
				util.P_out("setting commit index to: %d (%v)", c_idx, ret)
			}
			util.P_out(":::::::::::::::: true!")
			return true
		}
		util.P_out("returning false , failure")
		return false
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

	// our own
	s.Syncs = 1

	// send appendEntry messages to each replica
	response := make(chan RaftMessage)
	go func() {
		for _, r := range s.Replicas {
			prevLogIndex := log.Top() - 1	// if replica hasn't caught up, it will fail
			prevLogTerm := log.GetTermFor(prevLogIndex)
			appendEntryRequest := CreateAppendEntriesMessage(s.MessageId(), s.CurrentTerm, s.Pid, prevLogIndex, prevLogTerm, []log.Entry{entry}, leaderCommit)
			r.SendRaftMessage(appendEntryRequest, response)
		}
	}()

	for {
		select {
			case msg := <-response: {
				util.P_out("%v", msg)
				if s.RespondToAppendEntryResponse(msg) {
					util.P_out("BREAKING>>>>>>>>>>>>>>>>>>>>>>>>>")
					return
				}
			}
			case <-s.Timer.TimeoutEvent: {
				util.P_out("heartbeat!")
				s.Timer.Reset(s.HeartbeatTimeout)

				go func() {
					for _, r := range s.Replicas {
						util.P_out("sending heartbeat to %v", r.HostAddress)
						// prevLogIndex := log.Top()
						// prevLogTerm := log.TopTerm()
						// leaderCommit := log.CommitIndex()
						// heartbeat := CreateAppendEntriesMessage(s.MessageId(), s.CurrentTerm, s.Pid, prevLogIndex, prevLogTerm, []log.Entry{}, leaderCommit)

						// replacing above with replica specific stuff
						prevLogIndex := r.GetMatchIndex()
						prevLogTerm := log.GetTermFor(prevLogIndex)
						leaderCommit := log.CommitIndex()
						entries := log.GetEntriesAfter(prevLogIndex)
						heartbeat := CreateAppendEntriesMessage(s.MessageId(), s.CurrentTerm, s.Pid, prevLogIndex, prevLogTerm, entries, leaderCommit)

						r.SendRaftMessage(heartbeat, response)
					}
				}()

				s.Timer.TimeoutAck <- true
			}
		}
	}
	util.P_out(">>>>>>>>>>>> RETURNING")
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
			voteRequest := CreateVoteRequestMessage(s.MessageId(), s.CurrentTerm, s.Pid, 0, 0)	//TODO
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
							util.P_out("got vote! numVotes is now: %d", numVotes)

							if numVotes >= s.Majority {
								util.P_out("Yahoo!")
								s.SetState(LEADER)
								return
							}
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
				s.Timer.TimeoutAck <- true
			}
			case msg := <-s.ClientInterface: {
				util.P_out("Not the leader, returning... %s", msg)
				s.ClientAck <- false
			}
		}
		// was initially here but break simply breaks out of the switch, not the for
		// if numVotes > s.Majority {
		// 	util.P_out("Yahoo!")
		// 	s.SetState(LEADER)
		// 	break
		// }
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
						prevLogIndex := r.GetMatchIndex()
						// prevLogTerm := log.TopTerm()
						prevLogTerm := log.GetTermFor(prevLogIndex)
						leaderCommit := log.CommitIndex()
						entries := log.GetEntriesAfter(prevLogIndex)
						heartbeat := CreateAppendEntriesMessage(s.MessageId(), s.CurrentTerm, s.Pid, prevLogIndex, prevLogTerm, entries, leaderCommit)
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
				util.P_out("done executing ====================================")
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
