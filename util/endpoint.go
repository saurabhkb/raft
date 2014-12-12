package util

import "fmt"

type Endpoint struct {
	Ipaddr string
	Port int
}

func (e Endpoint) String() string {
	return fmt.Sprintf("%s:%d", e.Ipaddr, e.Port)
}

func (e Endpoint) Tcpformat() string {
	return fmt.Sprintf("tcp://%s:%d", e.Ipaddr, e.Port)
}

func (e Endpoint) RepTcpFormat() string {
	return fmt.Sprintf("tcp://%s:%d", e.Ipaddr, e.Port + 1)
}

func (e Endpoint) RaftTcpFormat() string {
	return fmt.Sprintf("tcp://%s:%d", e.Ipaddr, e.Port + 6)
}

func (e Endpoint) ClientTcpFormat() string {
	return fmt.Sprintf("tcp://%s:%d", e.Ipaddr, e.Port + 7)
}
