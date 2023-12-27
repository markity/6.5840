package raft

import (
	"sync"
)

// Debugging
const doDebug = false

type unboundedQueue struct {
	m sync.Mutex
	c *sync.Cond
	q []interface{}
}

func newUnboundedQueue() *unboundedQueue {
	q := &unboundedQueue{}
	q.c = sync.NewCond(&q.m)
	q.q = make([]interface{}, 0)
	return q
}

func (uq *unboundedQueue) Push(i interface{}) {
	uq.m.Lock()
	uq.q = append(uq.q, i)
	uq.m.Unlock()
	uq.c.Broadcast()
}

func (uq *unboundedQueue) PopAll() []interface{} {
	for {
		uq.m.Lock()
		if len(uq.q) == 0 {
			uq.c.Wait()
			uq.c.L.Unlock()
			continue
		} else {
			tmp := uq.q
			uq.q = make([]interface{}, 0)
			uq.m.Unlock()
			return tmp
		}
	}
}

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a int, b int) int {
	if a < b {
		return b
	}
	return a
}
