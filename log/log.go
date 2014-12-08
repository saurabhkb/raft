package log

/*
For the log, indexing starts at 1

Theres basically two levels of logs at work here:
1. VolatileLog -> this is the volatile log to which stuff is written initially (lost on crashes)
2. Put, Get interface -> this allows stuff to be written to stable storage (should be done only when that stuff is committed)
*/

import (
	"raft/storage"
	"strconv"
	"fmt"
)

var Name string
var commitIndex int
var VolatileLog []Entry

func Init(name string) {
	Name = name
	commitIndex = 0
	VolatileLog = []Entry{}

	_, err := storage.Get(fmt.Sprintf("%s:size", Name))
	if err != nil {
		storage.Put(fmt.Sprintf("%s:size", Name), "0")
	}
}

func Append(entry Entry) bool {
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
			commitIndex = idx
		}
	}
	return true
}

func CommitIndex() int {
	return commitIndex
}

func NextIndex() int {
	return len(VolatileLog)
}

func Top() int {
	return len(VolatileLog) - 1
}

func TopTerm() int {
	if len(VolatileLog) == 0 {
		return 0
	} else {
		return VolatileLog[len(VolatileLog) - 1].Term
	}
}

/*
* log = log[:idx] (if log[idx] isn't committed)
*/
func Truncate(idx, term int) bool {
	if idx < commitIndex || idx > len(VolatileLog) {
		return false
	}

	if idx > 0 {
		if VolatileLog[idx - 1].Term != term {
			return false
		} else {
			VolatileLog = VolatileLog[:idx]
		}
	} else {
		VolatileLog = []Entry{}
	}
	return true
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

func Size() int {
	ret, _ := storage.Get(fmt.Sprintf("%s:size", Name))
	size, _ := strconv.Atoi(ret)
	return size
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
