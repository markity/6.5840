package kvraft

import (
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labrpc"
)

const ClientTimeoutSeconds = 2

// 每个客户端需要得到一个全球唯一的clientID, 这里用原子操作做到
// 实际生产场景会有一个分布式id生成器
var clientIDGen int64 = 0

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

	// 我们需要使客户端请求是串行的, 用锁保证
	// It's OK to assume that a client will make
	// only one call into a Clerk at a time.
	mu sync.Mutex

	// 当前leader, 用于快速找leader
	currentLeaderID int
	// 每个客户端需要生成一个唯一的clientID
	clientID int
	// 单调递增的请求id
	reqID int64
}

func (ck *Clerk) changeLeader() {
	ck.currentLeaderID = (ck.currentLeaderID + 1) % len(ck.servers)
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers

	ck.clientID = int(atomic.AddInt64(&clientIDGen, 1))
	ck.reqID = 0

	dPrintf("make clerk %v", ck.clientID)

	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	dPrintf("[clerk%v] Get is called", ck.clientID)
	ck.mu.Lock()
	defer ck.mu.Unlock()

	getArgs := GetArgs{
		Key:      key,
		ClientID: int(ck.clientID),
		ReqID:    atomic.AddInt64(&ck.reqID, 1),
	}
	for {
		c := make(chan GetReply)
		go func() {
			var reply GetReply
			ok := ck.servers[ck.currentLeaderID].Call("KVServer.Get", &getArgs, &reply)
			if !ok {
				reply.Err = ErrNetworkNotOK
			}
			c <- reply
		}()
		select {
		case <-time.After(time.Second * ClientTimeoutSeconds):
			ck.changeLeader()
			time.Sleep(time.Millisecond)
			continue
		case result := <-c:
			switch result.Err {
			case OK:
				dPrintf("[clerk%v] get response", ck.clientID)
				return result.Value
			case ErrNoKey:
				return ""
			case ErrWrongLeader, ErrNetworkNotOK:
				ck.changeLeader()
				time.Sleep(time.Millisecond)
				dPrintf("[clerk%v] get retry", ck.clientID)
				continue
			default:
				panic("checkme")
			}
		}
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	dPrintf("[clerk%v] PutAppend is called", ck.clientID)
	ck.mu.Lock()
	defer ck.mu.Unlock()

	putArgs := PutAppendArgs{
		Key:      key,
		ClientID: int(ck.clientID),
		ReqID:    atomic.AddInt64(&ck.reqID, 1),
		Value:    value,
		Op:       op,
	}
	for {
		c := make(chan PutAppendReply)
		go func() {
			var reply PutAppendReply
			ok := ck.servers[ck.currentLeaderID].Call("KVServer.PutAppend", &putArgs, &reply)
			if !ok {
				reply.Err = ErrNetworkNotOK
			}
			c <- reply
		}()
		select {
		case <-time.After(time.Second * ClientTimeoutSeconds):
			ck.changeLeader()
			time.Sleep(time.Millisecond)
			continue
		case result := <-c:
			switch result.Err {
			case OK:
				dPrintf("[clerk%v] PutAppend return", ck.clientID)
				return
			case ErrWrongLeader, ErrNetworkNotOK:
				ck.changeLeader()
				time.Sleep(time.Millisecond)
				dPrintf("[clerk%v] PutAppend retry", ck.clientID)
				continue
			default:
				panic("checkme")
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
