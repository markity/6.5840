package raft

import (
	"bytes"

	"6.5840/labgob"
)

type LogEntry struct {
	LogTerm  int         `json:"log_term"`
	LogIndex int         `json:"log_index"`
	Command  interface{} `json:"command"`
}

func (le *LogEntry) ToBytes() []byte {
	buf := bytes.NewBuffer(nil)
	en := labgob.NewEncoder(buf)
	en.Encode(*le)

	return buf.Bytes()
}

type Logs []LogEntry

func BytesToLogEntry(bs []byte) (LogEntry, bool) {
	le := LogEntry{}
	de := labgob.NewDecoder(bytes.NewReader(bs))
	err := de.Decode(&le)
	return le, err == nil
}

func (l *Logs) LastLog() LogEntry {
	return (*l)[len(*l)-1]
}

// 0 1 2 3
func (l *Logs) FindLogByIndex(id int) (LogEntry, bool) {
	if len(*l)-1 < id {
		return LogEntry{}, false
	}
	return (*l)[id], true
}

func (l *Logs) At(id int) LogEntry {
	return (*l)[id]
}

func (l *Logs) LastEntryIndex() int {
	return len(*l) - 1
}

// 阶段后面的指定id以及后面的所有日志
func (l *Logs) TruncateBy(id int) {
	*l = (*l)[:id]
}

func (l *Logs) Append(e LogEntry) {
	*l = append(*l, e)
}

func (l *Logs) Copy() Logs {
	lo := Logs{}
	for _, v := range *l {
		lo.Append(v)
	}
	return lo
}
