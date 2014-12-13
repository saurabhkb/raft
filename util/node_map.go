package util

import (
	"strconv"
)

/*
* go through the hassle of string keys so that it becomes possible to json serialize
*/
type NodeMap map[string]Endpoint

func (n *NodeMap) Majority() int {
	return len(*n) / 2 + 1
}

func (n *NodeMap) Clear() {
	for k, _ := range *n {
		delete(*n, k)
	}
}

func (n *NodeMap) AddNode(pid int, endpoint Endpoint) {
	strPid := strconv.Itoa(pid)
	(*n)[strPid] = endpoint
}

func (n *NodeMap) GetKeys() []int {
	l := []int{}
	for k, _ := range *n {
		int_k, _ := strconv.Atoi(k)
		l = append(l, int_k)
	}
	return l
}

func (n *NodeMap) GetNode(pid int) Endpoint {
	strPid := strconv.Itoa(pid)
	return (*n)[strPid]
}

type BoolMap map[int]bool

func (b *BoolMap) Set(pid int, val bool) {
	(*b)[pid] = val
}

func (b *BoolMap) Clear() {
	for k, _ := range *b {
		delete(*b, k)
	}
}

func (b *BoolMap) Get(pid int) (bool, bool) {
	ret, found := (*b)[pid]
	return ret, found
}
