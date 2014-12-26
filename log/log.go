package log

/*
For the log, indexing starts at 1

Theres basically two levels of logs at work here:
1. VolatileLog -> this is the volatile log to which stuff is written initially (lost on crashes)
2. Put, Get interface -> this allows stuff to be written to stable storage (should be done only when that stuff is committed)
*/

import (
	//"raft/storage"
	"raft/util"
	//"strconv"
	"fmt"
	"sync"
	"os"
	"bufio"
)

const (
	ERROR_IDX_LT_COMMIT = "ERROR_IDX_LT_COMMIT"
	ERROR_IDX_GT_COMMIT = "ERROR_IDX_GT_COMMIT"
	ERROR_MISMATCHED_TERMS = "ERROR_MISMATCHED_TERMS"
	TRUNCATE_SUCCESS = "TRUNCATE_SUCCESS"
)

var Name string
var Pid int
var commitIndex int
var VolatileLog []Entry
var dummy Entry
var lock *sync.Mutex

func Init(name string, pid int) {
	Name = name
	Pid = pid
	dummy = CreateValueEntry(0, "dummy")
	VolatileLog = []Entry{dummy}
	lock = &sync.Mutex{}

	//initSize()
	_size := fsize()

	// reload saved log
	for i := 1; i <= _size; i++ {
		// VolatileLog = append(VolatileLog, get(i))
		VolatileLog = append(VolatileLog, fget(i))
	}
	commitIndex = fsize()
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
			fappend(entry)
			commitIndex = i
			util.P_out("COMMIT [%v] commitIndex=%d", entry, commitIndex)
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
	for i := 1; i <= fsize(); i++ {
		l = append(l, fget(i))
	}
	return l
}


func Truncate(idx, term int) string {
	lock.Lock()
	defer lock.Unlock()
	if idx < commitIndex {
		return ERROR_IDX_LT_COMMIT
	}
	if idx > len(VolatileLog) - 1 {
		return ERROR_IDX_GT_COMMIT
	}

	if idx > 0 {
		if VolatileLog[idx].Term != term {
			return ERROR_MISMATCHED_TERMS
		} else {
			VolatileLog = VolatileLog[:idx + 1]	// include idx in the thing to be kept
		}
	} else {
		VolatileLog = []Entry{dummy}
	}
	return TRUNCATE_SUCCESS
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
* FILE interface
*/
func fget(idx int) Entry {
	file, err := os.Open(fmt.Sprintf("log-%d", Pid))
	if err != nil {
		return Entry{}
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	i := 1
	for scanner.Scan() {
		if i == idx {
			line := scanner.Text()
			return fromJson(line)
		}
		i++
	}
	return Entry{}
}

func fappend(e Entry) {
	file, err := os.OpenFile(fmt.Sprintf("log-%d", Pid), os.O_APPEND | os.O_WRONLY | os.O_CREATE, 0666)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	json_str := toJson(e)
	file.WriteString(json_str + "\n")
}

func fsize() int {
	file, _ := os.OpenFile(fmt.Sprintf("log-%d", Pid), os.O_RDONLY, 0666)
	fileScanner := bufio.NewScanner(file)
	lineCount := 0
	for fileScanner.Scan() {
		lineCount++
	}
	return lineCount
}

/*
* These probably shouldn't be exposed
* They actually access the storage interface
*/
// func get(idx int) Entry {
// 	val, _ := storage.Get(fmt.Sprintf("%s:%d", Name, idx))
// 	return fromJson(val)
// }
// 
// func put(idx int, entry Entry) {
// 	storage.Put(fmt.Sprintf("%s:%d", Name, idx), toJson(entry))
// }
// 
// func size() int {
// 	ret, _ := storage.Get(fmt.Sprintf("%s:size", Name))
// 	_size, _ := strconv.Atoi(ret)
// 	return _size
// }
// 
// func initSize() {
// 	_, err := storage.Get(fmt.Sprintf("%s:size", Name))
// 	if err != nil {
// 		storage.Put(fmt.Sprintf("%s:size", Name), "0")
// 	}
// }
// 
// func incrSize() {
// 	sizeStr, err := storage.Get(fmt.Sprintf("%s:size", Name))
// 	if err == nil {
// 		size, _ := strconv.Atoi(sizeStr)
// 		incrSize := strconv.Itoa(size + 1)
// 		storage.Put(fmt.Sprintf("%s:size", Name), incrSize)
// 	} else {
// 		storage.Put(fmt.Sprintf("%s:size", Name), "1")
// 	}
// }

// func Close() {
// 	storage.Close()
// }
