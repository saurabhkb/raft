package main

import (
	"sync"
	"fmt"
	"strconv"
)

const (
	C_OLD = "c_old"
	C_NEW = "c_new"
	C_OLD_NEW = "c_old_new"
)


type Configuration struct {
	OldConfig NodeMap
	NewConfig NodeMap
	state string
	Lock *sync.Mutex
}

func (c *Configuration) Init() {
	c.OldConfig = NodeMap{}
	c.NewConfig = NodeMap{}
	c.state = C_OLD
	c.Lock = &sync.Mutex{}
}

func (c *Configuration) GetOldNewConfigAsString() string {
	return fmt.Sprintf("Size:%d", len(c.OldConfig))
}

func (c *Configuration) GetNewConfigAsString() string {
	return fmt.Sprintf("Size:%d", len(c.NewConfig))
}

// the old configuration can now be discarded
func (c *Configuration) ConsensusComplete() {
	c.OldConfig = c.NewConfig
}

func (c *Configuration) GetOldConfigPids() []int {
	l := []int{}
	for k := range c.OldConfig {
		intk, err := strconv.Atoi(k)
		fmt.Println("ERRL", err)
		l = append(l, intk)
	}
	return l
}

func (c *Configuration) GetNewConfigPids() []int {
	l := []int{}
	for k := range c.NewConfig {
		intk, _ := strconv.Atoi(k)
		l = append(l, intk)
	}
	return l
}

func (c *Configuration) SetState(st string) {
	c.Lock.Lock()
	defer c.Lock.Unlock()
	c.state = st
}

func (c *Configuration) State() string {
	c.Lock.Lock()
	defer c.Lock.Unlock()
	return c.state
}
