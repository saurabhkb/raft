package main

import (
	"raft/util"
	zmq "github.com/pebbe/zmq4"
)


// how i refer to my friends on the consensus group
type Replica struct {
	HostAddress util.Endpoint
	VoteResponded bool
	VoteGranted bool
	NextIndex int
	MatchIndex int

}

func CreateReplica(addr util.Endpoint) *Replica {
	r := &Replica{}
	r.HostAddress = addr
	r.VoteResponded = false
	r.VoteGranted = false
	r.NextIndex = 0
	r.MatchIndex = 0

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
		replyChan <- msg
	}()
}
