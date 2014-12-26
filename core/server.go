package core

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
	ReplicaLock *sync.Mutex
	currentTerm int

	state string
	Lock *sync.Mutex

	VotedFor bool
	Timeout int
	HeartbeatTimeout int
	Timer *ElectionTimer

	Sresponder *Responder

	ClientInterface chan string
	ClientAck chan RaftMessage

	nodeReplyMap *util.BoolMap

	messageCounter int

	Config *Configuration
	ConfigChangeNotifier chan RaftMessage

	LeaderPid int
}

/*
* Majority (accounting for joint consensus) -> does the passed nodemap constitute a quorum?
*/
func (s *Server) IsMajority(nmap *util.BoolMap) bool {
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
		return currentOld >= quorumOld
	}
	if s.Config.State() == C_OLD_NEW {
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
	// if the candidate's last log index is not at least as up to date as our own, rejecr
	// this was initially after the term checks
	if log.Top() > voteRequest.LastLogIndex || log.TopTerm() > voteRequest.LastLogTerm {
		return CreateVoteResponse(voteRequest.Id, s.CurrentTerm(), s.Pid, false)
	}

	// if stale term, reject it
	if s.CurrentTerm() > voteRequest.Term {
		return CreateVoteResponse(voteRequest.Id, s.CurrentTerm(), s.Pid, false)
	}

	// if term is newer than ours, update our term and demote self to follower
	if s.CurrentTerm() < voteRequest.Term {
		s.SetState(FOLLOWER)
		s.SetCurrentTerm(voteRequest.Term)
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
		return CreateAppendEntriesResponse(appendEntryRequest.Id, s.CurrentTerm(), s.Pid, log.CommitIndex(), false)
	}

	// if term is newer than ours, update our term and demote self to follower
	if appendEntryRequest.Term > s.CurrentTerm() {
		s.SetCurrentTerm(appendEntryRequest.Term)
	}

	s.LeaderPid = appendEntryRequest.FromPid

	s.Timer.Reset(s.Timeout)
	s.SetState(FOLLOWER)
	s.VotedFor = false

	// reset timer
	s.Timer.Reset(s.Timeout)

	status_code := log.Truncate(appendEntryRequest.PrevLogIndex, appendEntryRequest.PrevLogTerm)
	switch status_code {
		case log.ERROR_IDX_LT_COMMIT: {
			return CreateAppendEntriesResponse(appendEntryRequest.Id, s.CurrentTerm(), s.Pid, log.CommitIndex(), true)
		}
		case log.ERROR_IDX_GT_COMMIT: {
			return CreateAppendEntriesResponse(appendEntryRequest.Id, s.CurrentTerm(), s.Pid, log.CommitIndex(), false)
		}
		case log.ERROR_MISMATCHED_TERMS: {
			return CreateAppendEntriesResponse(appendEntryRequest.Id, s.CurrentTerm(), s.Pid, log.CommitIndex(), false)
		}
		default: {
		}
	}

	for i := 0; i < len(appendEntryRequest.Entries); i++ {
		success := log.Append(appendEntryRequest.Entries[i])
		if !success {
			return CreateAppendEntriesResponse(appendEntryRequest.Id, s.CurrentTerm(), s.Pid, log.CommitIndex(), false)
		}
	}

	success := log.SetCommitIndex(appendEntryRequest.LeaderCommit)
	if !success {
		return CreateAppendEntriesResponse(appendEntryRequest.Id, s.CurrentTerm(), s.Pid, log.CommitIndex(), false)
	}

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

			if !s.IsMajority(s.nodeReplyMap) {
				return false
			}

			var indices []int
			indices = append(indices, log.Top())
			s.ReplicaLock.Lock()
			for _, r := range s.Replicas {
				indices = append(indices, r.GetMatchIndex())
			}
			s.ReplicaLock.Unlock()
			sort.Ints(indices)

			c_idx := indices[s.Config.OldConfig.Majority() - 1]	// TODO for now, take the smaller commit index just to be sure (in case it is joint consensus)
			if c_idx > log.CommitIndex() {
				log.SetCommitIndex(c_idx)
			}
			return true
		}
		return false
	}
}


/*
* Client Interface
*/
func (s *Server) ExecuteClient(value string) RaftMessage {
	entry := log.CreateValueEntry(s.CurrentTerm(), value)
	return s.execute(entry)
}
func (s *Server) ExecuteSize(size, vtype int) RaftMessage {
	entry := log.CreateSizeEntry(s.CurrentTerm(), size, vtype)
	return s.execute(entry)
}
// will try to append the value to the log and commit
// TODO => probably need a separate nodeReplyMap, etc. for joint consensus since it needs to take place in parallel with normal client commits
func (s *Server) execute(entry log.Entry) RaftMessage {
	if s.State() != LEADER {
		return CreateClientSizeResponse(s.MessageId(), false)
	}

	// je suis le leader
	// entry := log.Entry{s.CurrentTerm(), value}
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
		s.ReplicaLock.Lock()
		for _, r := range s.Replicas {
			prevLogIndex := log.Top() - 1	// if replica hasn't caught up, it will fail
			prevLogTerm := log.GetTermFor(prevLogIndex)
			appendEntryRequest := CreateAppendEntriesMessage(s.MessageId(), s.CurrentTerm(), s.Pid, prevLogIndex, prevLogTerm, []log.Entry{entry}, leaderCommit)
			r.SendRaftMessage(appendEntryRequest, response)
		}
		s.ReplicaLock.Unlock()
	}()

	for {
		select {
			case msg := <-response: {
				if s.RespondToAppendEntryResponse(msg) {
					return CreateClientValueResponse(s.MessageId(), true)
				}
			}
			case <-s.Timer.TimeoutEvent: {
				s.Timer.Reset(s.HeartbeatTimeout)

				go func() {
					s.ReplicaLock.Lock()
					for _, r := range s.Replicas {
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
					s.ReplicaLock.Unlock()
				}()

				s.Timer.TimeoutAck <- true
			}
		}
	}
	// probably shouldn't come here
	return CreateClientValueResponse(s.MessageId(), false)
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
		s.ReplicaLock.Lock()
		for _, r := range s.Replicas {
			voteRequest := CreateVoteRequestMessage(s.MessageId(), s.CurrentTerm(), s.Pid, log.Top(), log.TopTerm())	//TODO
			r.SendRaftMessage(voteRequest, response)
		}
		s.ReplicaLock.Unlock()
	}()

	s.nodeReplyMap.Clear()
	s.VotedFor = true	// i voted for myself
	s.nodeReplyMap.Set(s.Pid, true)
	// numVotes := 1
	for s.State() == CANDIDATE {
		select {
			case msg := <-response: {
				switch msg.Type {
					case RAFT_VOTE_REP: {
						if msg.VoteGranted {
							s.nodeReplyMap.Set(msg.FromPid, true)

							if s.IsMajority(s.nodeReplyMap) {
								s.SetState(LEADER)
								s.nodeReplyMap.Clear()
								return
							}
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
				s.Timer.Reset(s.Timeout)
				s.Timer.TimeoutAck <- true
			}
			case <-s.ClientInterface: {
				s.ClientAck <- CreateDiffLeaderResponse(s.MessageId(), s.LeaderPid)
			}
		}
	}
}


func (s *Server) MoveIntoJointConsensus(newSize int) {
	util.SetConfigFile("config.txt")
	endpoints := util.ReadAllEndpoints(newSize)
	s.ReplicaLock.Lock()
	s.Replicas = []*Replica{}
	for _, e := range endpoints {
		if e != s.HostAddress {
			s.Replicas = append(s.Replicas, CreateReplica(e))
		}
		s.Config.NewConfig.AddNode(util.GetPidFromEndpoint(e), e)
	}
	s.ReplicaLock.Unlock()
	s.Config.SetState(C_OLD_NEW)
}

func (s *Server) MoveIntoNewConfiguration() {
	// once it commits, set state as NEW
	s.Config.SetState(C_NEW)

	// update Replicas -> only the new ones now
	s.ReplicaLock.Lock()
	s.Replicas = []*Replica{}
	for _, e := range s.Config.GetNewEndpoints() {
		if e != s.HostAddress {
			s.Replicas = append(s.Replicas, CreateReplica(e))
		}
	}
	s.ReplicaLock.Unlock()
}

func (s *Server) LeaderStart() {
	// send a heartbeat TODO
	s.Timer.Reset(s.HeartbeatTimeout)
	response := make(chan RaftMessage)
	// initialize nextIndex state of each replica to log.Top()
	s.ReplicaLock.Lock()
	for _, r := range s.Replicas {
		r.SetNextIndex(log.Top())
	}
	s.ReplicaLock.Unlock()
	for s.State() == LEADER {
		s.LeaderPid = s.Pid
		select {
			case msg := <-s.ConfigChangeNotifier: {
				go func() {
					s.MoveIntoJointConsensus(msg.Size)

					// execute joint consensus message
					s.ExecuteSize(len(s.Config.NewConfig), log.ENTRY_OLD_NEW)

					// once that is committed, move into new configuration
					s.MoveIntoNewConfiguration()

					// execute new quorum message
					s.ExecuteSize(len(s.Config.NewConfig), log.ENTRY_NEW)

					// the dance is over: the new state is now the old state TODO
					s.Config.ConsensusComplete()
				}()
			}
			case msg := <-response: {
				util.P_out("RECV %v", msg)
				if msg.Term > s.CurrentTerm() {
					s.SetCurrentTerm(msg.Term)
					s.SetState(FOLLOWER)
					s.Timer.Reset(s.Timeout)
					s.VotedFor = false
					return
				}
			}
			case <-s.Timer.TimeoutEvent: {
				s.Timer.Reset(s.HeartbeatTimeout)

				go func() {
					s.ReplicaLock.Lock()
					for _, r := range s.Replicas {
						prevLogIndex := r.GetMatchIndex()
						prevLogTerm := log.GetTermFor(prevLogIndex)
						leaderCommit := log.CommitIndex()
						entries := log.GetEntriesAfter(prevLogIndex)
						heartbeat := CreateAppendEntriesMessage(s.MessageId(), s.CurrentTerm(), s.Pid, prevLogIndex, prevLogTerm, entries, leaderCommit)
						r.SendRaftMessage(heartbeat, response)
					}
					s.ReplicaLock.Unlock()
				}()

				s.Timer.TimeoutAck <- true
			}
			case msg := <-s.Sresponder.ReceiveEvent: {
				util.P_out("RECV %v", msg)
				var reply RaftMessage
				switch msg.Type {
					case RAFT_APPEND_REQ: {
						s.Timer.Reset(s.Timeout)
						reply = s.RespondToAppendEntry(msg)
					}
					case RAFT_APPEND_REP: {
					}
					case RAFT_VOTE_REQ: {
						s.Timer.Reset(s.Timeout)
						reply = s.RespondToRequestVote(msg)
					}
					case RAFT_VOTE_REP: {
					}
				}
				s.Sresponder.SendChannel <- reply
			}
			case val := <-s.ClientInterface: {
				response := s.ExecuteClient(val)
				s.ClientAck <- response
			}
		}
	}
}

func (s *Server) FollowerStart() {
	for s.State() == FOLLOWER {
		select {
			case <-s.Timer.TimeoutEvent: {
				s.Timer.Reset(s.Timeout)
				s.Timer.TimeoutAck <- true
				s.SetState(CANDIDATE)
				break
			}
			case msg := <-s.Sresponder.ReceiveEvent: {
				var reply RaftMessage
				util.P_out("RECV %v", msg)
				switch msg.Type {
					case RAFT_APPEND_REQ: {
						// check the entry type (if it is a SIZE request, update our Replicas) and change our state to joint consensus
						// From the paper:
						// "a server always uses the latest configuration in its log, regardless of whether the entry is committed"
						// so we don't care if it commits or not, change our configuration anyway
						for _, e := range msg.Entries {
							if e.Vtype == log.ENTRY_OLD_NEW {
								if s.Config.State() == C_OLD {
									// move into joint consensus
									s.MoveIntoJointConsensus(e.Size)
								}
							} else if e.Vtype == log.ENTRY_NEW {
								if s.Config.State() == C_OLD_NEW {
									s.MoveIntoNewConfiguration()
									s.Config.ConsensusComplete()
								}
							}
						}
						s.Timer.Reset(s.Timeout)
						reply = s.RespondToAppendEntry(msg)
					}
					case RAFT_VOTE_REQ: {
						s.Timer.Reset(s.Timeout)
						reply = s.RespondToRequestVote(msg)
					}
				}
				s.Sresponder.SendChannel <- reply
			}
			case <-s.ClientInterface: {
				s.ClientAck <- CreateDiffLeaderResponse(s.MessageId(), s.LeaderPid)
			}
		}
	}
}

/*
*	Control functions
*/
func (s *Server) Init(Name string, Pid int, HostAddress util.Endpoint, ElectionTimeout int, nmap *util.NodeMap, interf chan string, ack chan RaftMessage, configChangeNotify chan RaftMessage) {
	s.Name = Name
	s.Pid = Pid
	s.HostAddress = HostAddress
	s.Timeout = ElectionTimeout
	s.HeartbeatTimeout = 2
	s.Lock = &sync.Mutex{}
	s.ReplicaLock = &sync.Mutex{}
	s.SetState(FOLLOWER)

	s.Timer = &ElectionTimer{}
	s.Timer.Init()
	s.Sresponder = &Responder{}
	s.Sresponder.Init(s.HostAddress.RepTcpFormat())

	s.ClientInterface = interf	// used to send commands to the server
	s.ClientAck = ack

	storage.Init("/tmp/raftdb/" + Name)
	log.Init(Name, Pid)

	s.nodeReplyMap = &util.BoolMap{}

	s.ConfigChangeNotifier = configChangeNotify
	s.Config = &Configuration{}
	s.Config.Init()

	s.Config.OldConfig.AddNode(s.Pid, s.HostAddress)	// add myself (since endpoints, pids probably contains only the other peers)
	s.ReplicaLock.Lock()
	for _, pid := range nmap.GetKeys() {
		e := nmap.GetNode(pid)
		if e != s.HostAddress {
			s.Replicas = append(s.Replicas, CreateReplica(e))
		}
		s.Config.OldConfig.AddNode(pid, e)
	}
	s.ReplicaLock.Unlock()

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
