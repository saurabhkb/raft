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

const DEFAULT_NUM_NODES = 3

func main() {
	namePtr := flag.String("name", "auto", "replica name")
	numPtr := flag.Int("n", DEFAULT_NUM_NODES, "number of machines")
	flag.Parse()
	util.SetConfigFile("config.txt")
	_, ServerName, Pid, _, _, HostAddress := util.GetConfigDetailsFromName(*namePtr)

	EndpointList := util.ReadAllEndpoints(*numPtr)
	init_nmap := NodeMap{}

	for _, e := range EndpointList {
		if e == HostAddress {
			continue
		}
		init_nmap.AddNode(util.GetPidFromEndpoint(e), e)
	}

	rand.Seed(time.Now().UnixNano() / int64(time.Second))
	TIMER_DURATION := int(float32(MIN_ELECTION_TIMEOUT) + rand.Float32() * float32(MAX_ELECTION_TIMEOUT - MIN_ELECTION_TIMEOUT))
	util.P_out("TIMER_DURATION: %d", TIMER_DURATION)

	configChangeNotify := make(chan RaftMessage)
	serverConnector := make(chan string)
	serverAck := make(chan bool)

	go func() {
		s := Server{}
		s.Init(ServerName, Pid, HostAddress, TIMER_DURATION, &init_nmap, serverConnector, serverAck, configChangeNotify)
		s.Start()
	}()

	// ideally, read stuff from a zmq socket; take from STDIN for now...
	for {
		var s string
		fmt.Printf("Input:")
		fmt.Scanf("%s", &s)
		util.P_out("read: %s", s)
		if s[0] == '#' {
			n := int(s[1] - '0')
			fmt.Println(n)
			util.SetConfigFile("config.txt")

			EndpointList := util.ReadAllEndpoints(n)
			nmap := &NodeMap{}
			for _, e := range EndpointList {
				if e == HostAddress {
					continue
				}
				nmap.AddNode(util.GetPidFromEndpoint(e), e)
			}
			configChangeNotify <- CreateClientSizeRequestMessage("configChange", n, *nmap)

		} else {
			serverConnector <- s
			<-serverAck
		}
	}
}
