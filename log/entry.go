package log

import (
	"encoding/json"
	"fmt"
)

const (
	_ = iota
	ENTRY_DUMMY
	ENTRY_SIZE
	ENTRY_CLIENT
	ENTRY_OLD_NEW
	ENTRY_NEW
)

type Entry struct {
	Vtype int
	Term int
	Value string
	Size int
}

func (e Entry) String() string {
	switch e.Vtype {
		case ENTRY_OLD_NEW: {
			return fmt.Sprintf("C_OLD_NEW Term:%d Size:%d", e.Term, e.Size)
		}
		case ENTRY_NEW: {
			return fmt.Sprintf("C_NEW Term:%d Size:%d", e.Term, e.Size)
		}
		case ENTRY_CLIENT: {
			return fmt.Sprintf("VALUE Term:%d Value:%s", e.Term, e.Value)
		}
		case ENTRY_SIZE: {
			return fmt.Sprintf("SIZE Term:%d Size:%d", e.Term, e.Size)
		}
		default: {
			return fmt.Sprintf("ENTRY Term:%d Value:%s", e.Term, e.Value)
		}
	}
}

func toJson(e Entry) string {
	b, _ := json.Marshal(e)
	return string(b)
}

func fromJson(s string) Entry {
	var e Entry
	json.Unmarshal([]byte(s), &e)
	return e
}

func CreateValueEntry(term int, value string) Entry {
	e := Entry{}
	e.Vtype = ENTRY_CLIENT
	e.Term = term
	e.Value = value
	return e
}

func CreateSizeEntry(term, size, vtype int) Entry {
	e := Entry{}
	e.Vtype = ENTRY_SIZE
	e.Size = size
	e.Term = term
	e.Vtype = vtype
	return e
}
