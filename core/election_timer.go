package core

import (
	"time"
	"sync"
	"raft/util"
)

const TIME_UNIT = 1

type ElectionTimer struct {
	lock *sync.Mutex
	TimeoutDuration int
	Time int
	TimeoutEvent chan bool
	TimeoutAck chan bool
	Quitter chan bool
}

func (e *ElectionTimer) Init() {
	e.lock = &sync.Mutex{}
	e.TimeoutEvent = make(chan bool)
	e.TimeoutAck = make(chan bool)
	e.Quitter = make(chan bool)
}

func (e *ElectionTimer) Start() {
	ticker := time.NewTicker(TIME_UNIT * time.Second)
	e.Time = e.TimeoutDuration
	go func() {
		for {
			select {
				case <-ticker.C: {
					e.lock.Lock()
					e.Time--
					util.P_out("time: %d", e.Time)
					if e.Time == 0 {
						e.lock.Unlock()

						// notify the election timer of the timeout
						e.TimeoutEvent <- true

						// wait for election timer's ack before continuing (most likely the timer will be reset)
						<-e.TimeoutAck
					} else {
						e.lock.Unlock()
					}
				}
				case <-e.Quitter: {
					break
				}
			}
		}
	}()
}

func (e *ElectionTimer) Reset(duration int) {
	e.lock.Lock()
	e.TimeoutDuration = duration
	e.Time = duration
	e.lock.Unlock()
}

func (e *ElectionTimer) Stop() {
	e.Quitter <- true
}
