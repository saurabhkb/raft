package log

import "encoding/json"

const (
	_ = iota
	ENTRY_DUMMY
	ENTRY_SIZE
	ENTRY_CLIENT
)

type Entry struct {
	Vtype int
	Term int
	Value string
	Size int
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

func CreateSizeEntry(term, size int) Entry {
	e := Entry{}
	e.Vtype = ENTRY_SIZE
	e.Size = size
	e.Term = term
	return e
}
