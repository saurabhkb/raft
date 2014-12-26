package core

import (
	zmq "github.com/pebbe/zmq4"
	"raft/util"
)

type Responder struct {
	ReceiveEvent chan RaftMessage
	SendChannel chan RaftMessage
}

func (r *Responder) Init(tcpAddr string) {
	r.ReceiveEvent = make(chan RaftMessage)
	r.SendChannel = make(chan RaftMessage)
	go func() {
		socket, _ := zmq.NewSocket(zmq.REP)
		socket.Bind(tcpAddr)
		for {
			byteRecv, _ := socket.Recv(0)
			m := FromJson(byteRecv)

			r.ReceiveEvent <-m
			tosend := <-r.SendChannel

			byteSend := tosend.ToJson()
			util.P_out("SEND %v", tosend)
			socket.Send(byteSend, 0)
		}
	}()
}
