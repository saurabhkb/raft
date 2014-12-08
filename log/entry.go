package log

import "encoding/json"

type Entry struct {
	Term int
	Value string
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


