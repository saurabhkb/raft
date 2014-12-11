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
	currentTerm int

	state string
	Lock *sync.Mutex

	VotedFor bool
	Timeout int
	HeartbeatTimeout int
	Timer *ElectionTimer

	Sresponder *Responder

	ClientInterface chan string
	ClientAck chan bool

	nodeReplyMap *BoolMap

	messageCounter int

	Config *Configuration
	ConfigChangeNotifier chan RaftMessage
}

/*
* Majority (accounting for joint consensus) -> does the passed nodemap constitute a quorum?
*/
func (s *Server) IsMajority(nmap *BoolMap) bool {
	currentOld := 0
	quorumOld := s.Config.OldConfig.Majority()
	currentNew := 0
	quorumNew := s.Config.NewConfig.Majority()

	for _, id := range s.Config.GetOldConfigPids() {
		if ret, found := nmap.Get(id); found && ret {
			currentOld++
		}
	}
	for _, id := range s.Config.GetNewConfigPids() {
		if ret, found := nmap.Get(id); found && ret {
			currentNew++
		}
	}
	if s.Config.State() == C_OLD {
		util.P_out("currentOld:%d, quorumOld:%d, old:%v", currentOld, quorumOld, s.Config.OldConfig)
		return currentOld >= quorumOld
	}
	if s.Config.State() == C_OLD_NEW {
		if currentNew >= quorumNew {
			util.P_out("=====> PASSED THE NEW CONFIGURATION QUORUM!")
		} else {
			util.P_out("=====> FAILED THE NEW CONFIGURATION QUORUM!")
		}
		if currentOld >= quorumOld {
			util.P_out("=====> PASSED THE OLD CONFIGURATION QUORUM!")
		} else {
			util.P_out("=====> FAILED THE OLD CONFIGURATION QUORUM!")
		}
		return (currentNew >= quorumNew) && (currentOld >= quorumOld)
	}
	if s.Config.State() == C_NEW {
		return currentNew >= quorumNew
	}
	return false
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

func (s *Server) CurrentTerm() int {
	s.Lock.Lock()
	defer s.Lock.Unlock()
	return s.currentTerm
}

func (s *Server) SetCurrentTerm(trm int) {
	s.Lock.Lock()
	defer s.Lock.Unlock()
	s.currentTerm = trm
}


/*
* Voting functions
*/
// can vote if I haven't voted before and the message term is >= mine
func (s *Server) RespondToRequestVote(voteRequest RaftMessage) RaftMessage {
	// if stale term, reject it
	if s.CurrentTerm() > voteRequest.Term {
		return CreateVoteResponse(voteRequest.Id, s.CurrentTerm(), s.Pid, false)
	}

	// if term is newer than ours, update our term and demote self to follower
	if s.CurrentTerm() < voteRequest.Term {
		s.SetState(FOLLOWER)
		s.SetCurrentTerm(voteRequest.Term)
	}

	// if the candidate's last log index is not at least as up to date as our own, reject
	if log.Top() > voteRequest.LastLogIndex || log.TopTerm() > voteRequest.LastLogTerm {
		util.P_out("!!!!!!!!!!! CANT VOTE FOR YOU! You aren't as recent as me!: %v", util.GetEndpointFromPid(voteRequest.FromPid))
		return CreateVoteResponse(voteRequest.Id, s.CurrentTerm(), s.Pid, false)
	}

	// if i haven't voted yet
	if !s.VotedFor {
		s.VotedFor = true
		return CreateVoteResponse(voteRequest.Id, s.CurrentTerm(), s.Pid, true)
	}

	return CreateVoteResponse(voteRequest.Id, s.CurrentTerm(), s.Pid, false)
}


/*
* Append Entry functions
*/
func (s *Server) RespondToAppendEntry(appendEntryRequest RaftMessage) RaftMessage {
	// if stale term, reject it
	if s.CurrentTerm() > appendEntryRequest.Term {
		util.P_out("stale term: %d > %d", s.CurrentTerm(), appendEntryRequest.Term)
		return CreateAppendEntriesResponse(appendEntryRequest.Id, s.CurrentTerm(), s.Pid, log.CommitIndex(), false)
	}

	// if term is newer than ours, update our term and demote self to follower
	if appendEntryRequest.Term > s.CurrentTerm() {
		s.SetCurrentTerm(appendEntryRequest.Term)
	}

	s.Timer.Reset(s.Timeout)
	s.SetState(FOLLOWER)
	s.VotedFor = false

	// reset timer
	s.Timer.Reset(s.Timeout)

	success := log.Truncate(appendEntryRequest.PrevLogIndex, appendEntryRequest.PrevLogTerm)
	if !success {
		util.P_out("cannot truncate properly")
		return CreateAppendEntriesResponse(appendEntryRequest.Id, s.CurrentTerm(), s.Pid, log.CommitIndex(), false)
	}

	for i := 0; i < len(appendEntryRequest.Entries); i++ {
		success = log.Append(appendEntryRequest.Entries[i])
		if !success {
			util.P_out("cannot append properly")
			return CreateAppendEntriesResponse(appendEntryRequest.Id, s.CurrentTerm(), s.Pid, log.CommitIndex(), false)
		}
	}

	success = log.SetCommitIndex(appendEntryRequest.LeaderCommit)
	if !success {
		util.P_out("")
		return CreateAppendEntriesResponse(appendEntryRequest.Id, s.CurrentTerm(), s.Pid, log.CommitIndex(), false)
	}

	util.P_out("%s", log.Stats())
	return CreateAppendEntriesResponse(appendEntryRequest.Id, s.CurrentTerm(), s.Pid, log.CommitIndex(), true)
}

// TODO not sure about the bool
func (s *Server) RespondToAppendEntryResponse(appendEntryResponse RaftMessage) bool {
	// if term is newer than ours, update our term and demote self to follower
	if appendEntryResponse.Term > s.CurrentTerm() {
		s.SetCurrentTerm(appendEntryResponse.Term)
		s.SetState(FOLLOWER)
		s.Timer.Reset(s.Timeout)
		s.VotedFor = false
		return true
	} else {
		if appendEntryResponse.Success {
			// s.Syncs++
			s.nodeReplyMap.Set(appendEntryResponse.FromPid, true)

			// if s.Syncs < s.Majority {
			// 	return false
			// }

			util.P_out("nodeReplyMap:%v", *(s.nodeReplyMap))
			if !s.IsMajority(s.nodeReplyMap) {
				util.P_out("no majority!")
				return false
			}

			var indices []int
			indices = append(indices, log.Top())
			for _, r := range s.Replicas {
				indices = append(indices, r.GetMatchIndex())
			}
			sort.Ints(indices)
			util.P_out("indices: %v", indices)

			c_idx := indices[s.Config.OldConfig.Majority() - 1]	// TODO for now, take the smaller commit index just to be sure (in case it is joint consensus)
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
	entry := log.Entry{s.CurrentTerm(), value}
	// prevLogIndex := log.Top()
	// prevLogTerm := log.TopTerm()
	leaderCommit := log.CommitIndex()
	log.Append(entry)

	// our own
	s.nodeReplyMap.Clear()
	s.nodeReplyMap.Set(s.Pid, true)
	// s.Syncs = 1

	// send appendEntry messages to each replica
	response := make(chan RaftMessage)
	go func() {
		for _, r := range s.Replicas {
			prevLogIndex := log.Top() - 1	// if replica hasn't caught up, it will fail
			prevLogTerm := log.GetTermFor(prevLogIndex)
			appendEntryRequest := CreateAppendEntriesMessage(s.MessageId(), s.CurrentTerm(), s.Pid, prevLogIndex, prevLogTerm, []log.Entry{entry}, leaderCommit)
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
						// heartbeat := CreateAppendEntriesMessage(s.MessageId(), s.CurrentTerm(), s.Pid, prevLogIndex, prevLogTerm, []log.Entry{}, leaderCommit)

						// replacing above with replica specific stuff
						prevLogIndex := r.GetMatchIndex()
						prevLogTerm := log.GetTermFor(prevLogIndex)
						leaderCommit := log.CommitIndex()
						entries := log.GetEntriesAfter(prevLogIndex)
						heartbeat := CreateAppendEntriesMessage(s.MessageId(), s.CurrentTerm(), s.Pid, prevLogIndex, prevLogTerm, entries, leaderCommit)

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

	// increment term
	cur := s.CurrentTerm()
	s.SetCurrentTerm(cur + 1)

	response := make(chan RaftMessage)
	go func() {
		for _, r := range s.Replicas {
			util.P_out("sending vote request to %v", r.HostAddress)
			voteRequest := CreateVoteRequestMessage(s.MessageId(), s.CurrentTerm(), s.Pid, log.Top(), log.TopTerm())	//TODO
			util.P_out("voteReq:%v", voteRequest)
			r.SendRaftMessage(voteRequest, response)
		}
	}()

	s.nodeReplyMap.Clear()
	s.VotedFor = true	// i voted for myself
	s.nodeReplyMap.Set(s.Pid, true)
	// numVotes := 1
	for s.State() == CANDIDATE {
		select {
			case msg := <-response: {
				util.P_out("initiate election: %v", msg)
				switch msg.Type {
					case VOTE_RES: {
						if msg.VoteGranted {
							s.nodeReplyMap.Set(msg.FromPid, true)
							util.P_out("got vote! numVotes is now: %v", *s.nodeReplyMap)

							if s.IsMajority(s.nodeReplyMap) {
								util.P_out("Yahoo!")
								s.SetState(LEADER)
								s.nodeReplyMap.Clear()
								return
							}
							// if numVotes >= s.Majority {
							// 	util.P_out("Yahoo!")
							// 	s.SetState(LEADER)
							// 	return
							// }
						} else {
							if s.CurrentTerm() < msg.Term {
								s.SetState(FOLLOWER)
								s.SetCurrentTerm(msg.Term)
								s.nodeReplyMap.Clear()
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
			case msg := <-s.ConfigChangeNotifier: {
				util.P_out("RECEIVED %v: Moving into joint consensus!;;;;;;;;;;;;;;;;;;;;;;;;;", msg)
				go func() {
					s.Config.SetState(C_OLD_NEW)
					util.P_out("configuration state is %s", s.Config.State())
					s.Execute(s.Config.GetOldNewConfigAsString())
					s.Config.SetState(C_NEW)
					s.Execute(s.Config.GetNewConfigAsString())
					// the dance is over: the new state is now the old state TODO
					s.Config.ConsensusComplete()
				}()
			}
			case msg := <-response: {
				util.P_out("on response, received: %v", msg)
				if msg.Term > s.CurrentTerm() {
					s.SetCurrentTerm(msg.Term)
					s.SetState(FOLLOWER)
					s.Timer.Reset(s.Timeout)
					s.VotedFor = false
					return
				}
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
						heartbeat := CreateAppendEntriesMessage(s.MessageId(), s.CurrentTerm(), s.Pid, prevLogIndex, prevLogTerm, entries, leaderCommit)
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
func (s *Server) Init(Name string, Pid int, HostAddress util.Endpoint, ElectionTimeout int, nmap *NodeMap, interf chan string, ack chan bool, configChangeNotify chan RaftMessage) {
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

	s.nodeReplyMap = &BoolMap{}

	s.ConfigChangeNotifier = configChangeNotify
	s.Config = &Configuration{}
	s.Config.Init()

	s.Config.OldConfig.AddNode(s.Pid, s.HostAddress)	// add myself (since endpoints, pids probably contains only the other peers)
	for _, pid := range nmap.GetKeys() {
		e := nmap.GetNode(pid)
		s.Replicas = append(s.Replicas, CreateReplica(e))
		s.Config.OldConfig.AddNode(pid, e)
	}
	util.P_out("%v", s.Config.OldConfig)

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
