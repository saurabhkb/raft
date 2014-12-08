package main

import (
	zmq "github.com/pebbe/zmq4"
	"encoding/json"
)

type Responder struct {
	socket *zmq.Socket
	ReceiveEvent chan RaftMessage
	SendChannel chan RaftMessage
}

func (r *Responder) Init(tcpAddr string) {
	r.socket, _ = zmq.NewSocket(zmq.REP)
	r.socket.Bind(tcpAddr)

	go func() {
		for {
			byteRecv, _ := r.socket.Recv(0)
			m := RaftMessage{}
			json.Unmarshal([]byte(byteRecv), &m)

			r.ReceiveEvent <-m
			tosend := <-r.SendChannel

			byteSend, _ := json.Marshal(tosend)
			r.socket.Send(string(byteSend[:]), 0)
		}
	}()
}
