package log

/*
For the log, indexing starts at 1

Theres basically two levels of logs at work here:
1. VolatileLog -> this is the volatile log to which stuff is written initially (lost on crashes)
2. Put, Get interface -> this allows stuff to be written to stable storage (should be done only when that stuff is committed)
*/

import (
	"raft/storage"
	"raft/util"
	"strconv"
	"fmt"
	"sync"
)

var Name string
var commitIndex int
var VolatileLog []Entry
var dummy Entry
var lock *sync.Mutex

func Init(name string) {
	Name = name
	dummy = Entry{-1, "DUMMY"}
	VolatileLog = []Entry{dummy}
	lock = &sync.Mutex{}

	sizeStr, err := storage.Get(fmt.Sprintf("%s:size", Name))
	_size, _ := strconv.Atoi(sizeStr)
	if err != nil {
		storage.Put(fmt.Sprintf("%s:size", Name), "0")
	}

	// reload saved log
	for i := 1; i <= _size; i++ {
		VolatileLog = append(VolatileLog, Get(i))
	}
	commitIndex = size()
	util.P_out("Reloaded log: %v, commit index: %d", VolatileLog, commitIndex)
}

func Append(entry Entry) bool {
	lock.Lock()
	defer lock.Unlock()
	volatileLogSize := len(VolatileLog)
	if volatileLogSize > 0 {
		if VolatileLog[volatileLogSize - 1].Term > entry.Term {
			return false
		}
	}
	VolatileLog = append(VolatileLog, entry)
	return true
}

func SetCommitIndex(idx int) bool {
	lock.Lock()
	defer lock.Unlock()
	// no duplicate committing
	if idx < commitIndex {
		return false
	}

	// idx is too far ahead
	if idx > len(VolatileLog) {
		return false
	}

	for i, entry := range VolatileLog {
		if i > commitIndex && i <= idx {
			Put(idx, entry)
			IncrSize()
			commitIndex = idx
		}
	}
	return true
}

func CommitIndex() int {
	lock.Lock()
	defer lock.Unlock()
	return commitIndex
}

func NextIndex() int {
	lock.Lock()
	defer lock.Unlock()
	return len(VolatileLog)
}

func Top() int {
	lock.Lock()
	defer lock.Unlock()
	return len(VolatileLog) - 1
}

func TopTerm() int {
	lock.Lock()
	defer lock.Unlock()
	if len(VolatileLog) == 0 {
		return 0
	} else {
		return VolatileLog[len(VolatileLog) - 1].Term
	}
}

func Stats() string {
	return fmt.Sprintf("Log Stats:\nVolatileEntries:%v\nSaved:%v\nTop:%d\nNextIndex:%d\nCommitIndex:%d\n", VolatileLog, Saved(), Top(), NextIndex(), CommitIndex())
}

func Saved() []Entry {
	l := []Entry{}
	for i := 0; i <= size(); i++ {
		l = append(l, Get(i))
	}
	return l
}

/*
* log = log[:idx] (if log[idx] isn't committed)
*/
func Truncate(idx, term int) bool {
	lock.Lock()
	defer lock.Unlock()
	util.P_out("before truncate: %v", VolatileLog)
	if idx < commitIndex || idx > len(VolatileLog) - 1 {	// can't use Top (it deadlocks)
		return false
	}

	if idx > 0 {
		if VolatileLog[idx].Term != term {
			return false
		} else {
			VolatileLog = VolatileLog[:idx + 1]	// include idx in the thing to be kept
		}
	} else {
		VolatileLog = []Entry{dummy}
	}
	util.P_out("after truncate: %v", VolatileLog)
	return true
}

func GetTermFor(idx int) int {
	lock.Lock()
	defer lock.Unlock()
	if idx >= len(VolatileLog) || idx < 0 {
		return -1
	} else {
		return VolatileLog[idx].Term
	}
}

func GetEntriesAfter(idx int) []Entry {
	list := []Entry{}
	if idx < 0 || idx > Top() {
		return []Entry{}
	} else {
		for i := idx + 1; i <= Top(); i++ {
			list = append(list, VolatileLog[i])
		}
	}
	return list
}



/*
* These probably shouldn't be exposed
*/
func Get(idx int) Entry {
	val, _ := storage.Get(fmt.Sprintf("%s:%d", Name, idx))
	return fromJson(val)
}

func Put(idx int, entry Entry) {
	storage.Put(fmt.Sprintf("%s:%d", Name, idx), toJson(entry))
}

func size() int {
	ret, _ := storage.Get(fmt.Sprintf("%s:size", Name))
	_size, _ := strconv.Atoi(ret)
	return _size
}

func IncrSize() {
	sizeStr, err := storage.Get(fmt.Sprintf("%s:size", Name))
	if err == nil {
		size, _ := strconv.Atoi(sizeStr)
		incrSize := strconv.Itoa(size + 1)
		storage.Put(fmt.Sprintf("%s:size", Name), incrSize)
	} else {
		storage.Put(fmt.Sprintf("%s:size", Name), "1")
	}
}

func Close() {
	storage.Close()
}
