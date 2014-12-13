package core

import (
	"raft/util"
	zmq "github.com/pebbe/zmq4"
	"sync"
)


// how i refer to my friends on the consensus group
type Replica struct {
	HostAddress util.Endpoint
	VoteResponded bool
	VoteGranted bool
	NextIndex int
	MatchTerm int
	MatchIndex int

	lock *sync.Mutex
}

func CreateReplica(addr util.Endpoint) *Replica {
	r := &Replica{}
	r.HostAddress = addr
	r.VoteResponded = false
	r.VoteGranted = false
	r.NextIndex = 1
	r.MatchTerm = 0
	r.MatchIndex = 0

	r.lock = &sync.Mutex{}

	return r
}

// cannot block
func (r *Replica) SendRaftMessage(req RaftMessage, replyChan chan RaftMessage) {
	go func() {
		socket, _ := zmq.NewSocket(zmq.REQ)
		socket.Connect(r.HostAddress.RepTcpFormat())

		s := req.ToJson()
		socket.Send(s, 0)
		response, _ := socket.Recv(0)
		msg := FromJson(response)
		r.ApplyUpdates(req, msg)
		replyChan <- msg
	}()
}

func (r *Replica) ApplyUpdates(req, res RaftMessage) {
	switch req.Type {
		case RAFT_APPEND_REQ: {
			if res.Success {
				if len(req.Entries) > 0 {
					// this is not a heartbeat
					r.SetMatchTerm(res.Term)
					// instead of doing this, lets set it to req.PrevLogIndex + len(req.Entries)
					// because if we get this multiple times, bad things happen
					//r.SetMatchIndex(r.MatchIndex + len(req.Entries))

					if r.MatchIndex < req.PrevLogIndex + len(req.Entries) {
						r.MatchIndex = req.PrevLogIndex + len(req.Entries)
					}

					r.SetNextIndex(r.MatchIndex + 1)
					// r.MatchTerm = res.Term
					// r.MatchIndex = r.MatchIndex + len(req.Entries)
					// r.NextIndex = r.MatchIndex + 1
					util.P_out("setting replica %v NextIndex to %d", r.HostAddress, r.NextIndex)
				}
			} else if res.Term == req.Term && res.LeaderCommit >= r.MatchIndex {
				r.SetMatchIndex(res.LeaderCommit)
				r.SetMatchTerm(res.Term)
				r.SetNextIndex(r.MatchIndex + 1)
				// r.MatchIndex = res.LeaderCommit
				// r.MatchTerm = res.Term
				// r.NextIndex = r.MatchIndex + 1
			} else {
				r.SetNextIndex(r.NextIndex - 1)
				r.SetMatchIndex(r.MatchIndex - 1)
			}
		}
	}
}

func (r *Replica) SetNextIndex(idx int) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.NextIndex = idx
}

func (r *Replica) SetMatchIndex(idx int) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.MatchIndex = idx
}

func (r *Replica) GetMatchIndex() int {
	r.lock.Lock()
	defer r.lock.Unlock()
	return r.MatchIndex
}

func (r *Replica) SetMatchTerm(trm int) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.MatchTerm = trm
}
