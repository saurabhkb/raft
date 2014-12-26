package main

import (
	"raft/util"
	"raft/core"
	"flag"
	"math/rand"
	"time"
	"fmt"
	zmq "github.com/pebbe/zmq4"
)

func ClientSocketLoop(HostAddress util.Endpoint, configChangeNotify chan core.RaftMessage, serverConnector chan string, serverAck chan core.RaftMessage) {
	clientSocket, _ := zmq.NewSocket(zmq.REP)
	clientSocket.Bind(HostAddress.ClientTcpFormat())
	for {
		msg, _ := clientSocket.Recv(0)
		clientMsg := core.FromJson(msg)

		util.P_out("received: %v", clientMsg)

		var reply core.RaftMessage
		switch clientMsg.Type {
			case core.RAFT_CLIENT_SIZE_REQ: {
				n := clientMsg.Ivalue
				util.SetConfigFile("config.txt")

				EndpointList := util.ReadAllEndpoints(n)
				nmap := &util.NodeMap{}
				for _, e := range EndpointList {
					nmap.AddNode(util.GetPidFromEndpoint(e), e)
				}
				configChangeNotify <- core.CreateClientSizeRequestMessage("configChange", n, *nmap)
				reply = <-serverAck
			}
			case core.RAFT_CLIENT_VALUE_REQ: {
				serverConnector <- clientMsg.Value
				reply = <-serverAck
			}
		}
		clientSocket.Send(reply.ToJson(), 0)
	}
}

func ClientStdinLoop(HostAddress util.Endpoint, configChangeNotify chan core.RaftMessage, serverConnector chan string, serverAck chan core.RaftMessage) {
	for {
		var s string
		fmt.Printf("Input:")
		fmt.Scanf("%s", &s)
		if s[0] == '#' {
			n := int(s[1] - '0')
			fmt.Println(n)
			util.SetConfigFile("config.txt")

			EndpointList := util.ReadAllEndpoints(n)
			nmap := &util.NodeMap{}
			for _, e := range EndpointList {
				nmap.AddNode(util.GetPidFromEndpoint(e), e)
			}
			configChangeNotify <- core.CreateClientSizeRequestMessage("configChange", n, *nmap)

		} else {
			serverConnector <- s
			<-serverAck
		}
	}
}


const MIN_ELECTION_TIMEOUT = 4
const MAX_ELECTION_TIMEOUT = 6

const DEFAULT_NUM_NODES = 3

func main() {
	namePtr := flag.String("name", "auto", "replica name")
	numPtr := flag.Int("n", DEFAULT_NUM_NODES, "number of machines")
	flag.Parse()
	util.SetConfigFile("config.txt")
	_, ServerName, Pid, _, _, HostAddress := util.GetConfigDetailsFromName(*namePtr)

	EndpointList := util.ReadAllEndpoints(*numPtr)
	init_nmap := util.NodeMap{}

	for _, e := range EndpointList {
		if e == HostAddress {
			continue
		}
		init_nmap.AddNode(util.GetPidFromEndpoint(e), e)
	}

	rand.Seed(time.Now().UnixNano() / int64(time.Second))
	TIMER_DURATION := int(float32(MIN_ELECTION_TIMEOUT) + rand.Float32() * float32(MAX_ELECTION_TIMEOUT - MIN_ELECTION_TIMEOUT))

	configChangeNotify := make(chan core.RaftMessage)
	serverConnector := make(chan string)
	serverAck := make(chan core.RaftMessage)

	go func() {
		s := core.Server{}
		s.Init(ServerName, Pid, HostAddress, TIMER_DURATION, &init_nmap, serverConnector, serverAck, configChangeNotify)
		s.Start()
	}()

	//ClientStdinLoop(HostAddress, configChangeNotify, serverConnector, serverAck)
	ClientSocketLoop(HostAddress, configChangeNotify, serverConnector, serverAck)
}
