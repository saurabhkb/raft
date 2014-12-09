package main

import (
	"raft/util"
	"flag"
	"math/rand"
	"time"
	"fmt"
)


const MIN_ELECTION_TIMEOUT = 4
const MAX_ELECTION_TIMEOUT = 6

func main() {
	namePtr := flag.String("name", "auto", "replica name")
	flag.Parse()
	util.SetConfigFile("config.txt")
	_, ServerName, Pid, _, _, HostAddress := util.GetConfigDetailsFromName(*namePtr)
	EndpointList := util.ReadAllEndpoints(HostAddress)

	rand.Seed(time.Now().UnixNano() / int64(time.Second))
	TIMER_DURATION := int(float32(MIN_ELECTION_TIMEOUT) + rand.Float32() * float32(MAX_ELECTION_TIMEOUT - MIN_ELECTION_TIMEOUT))
	util.P_out("TIMER_DURATION: %d", TIMER_DURATION)

	serverConnector := make(chan string)
	serverAck := make(chan bool)

	go func() {
		s := Server{}
		s.Init(ServerName, Pid, HostAddress, TIMER_DURATION, EndpointList, serverConnector, serverAck)
		s.Start()
	}()

	// ideally, read stuff from a zmq socket; take from STDIN for now...
	for {
		var s string
		fmt.Printf("Input:")
		fmt.Scanf("%s", &s)
		util.P_out("read: %s", s)
		serverConnector <- s
		<-serverAck
	}
}
