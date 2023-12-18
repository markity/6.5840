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

// 0 1 2 3 4 5
// 1 2 3 4 5 6
func (l *Logs) FindLogByIndex(idx int) (LogEntry, bool) {
	realIdx := idx - (*l)[0].LogIndex
	if realIdx < 0 || len(*l)-1 < realIdx {
		return LogEntry{}, false
	}
	return (*l)[realIdx], true
}

func (l *Logs) At(id int) LogEntry {
	return (*l)[id]
}

// 如果日志为空, 返回lastIncludedIndex
func (l *Logs) LastLogIndex() int {
	return (*l)[len(*l)-1].LogIndex
}

// 4 5 6 7 8
func (l *Logs) GetByIndex(idx int) LogEntry {
	return (*l)[idx-(*l)[0].LogIndex]
}

// 剪切日志, 但是包含那个日志, 把idx对应日志的command变为空
func (l *Logs) TrimLogs(idx int) {
	i := l.GetByIndex(idx).LogIndex - (*l)[0].LogIndex
	(*l) = (*l)[i:]
	(*l)[0].Command = nil
}

// 截断后面的指定id以及后面的所有日志
func (l *Logs) TruncateBy(id int) {
	*l = (*l)[:id-(*l)[0].LogIndex]
}

func (l *Logs) Append(e LogEntry) {
	e.LogIndex = l.LastLog().LogIndex + 1
	*l = append(*l, e)
}

func (l *Logs) Copy() Logs {
	lo := Logs{}
	lo = append(lo, *l...)
	return lo
}
