package kvraft

import (
	"bytes"
	"log"
	"sync"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

func dPrintf(format string, a ...interface{}) (n int, err error) {
	if debug {
		log.Printf(format, a...)
	}
	return
}

type commandType int

const (
	CommandTypePut commandType = iota
	CommandTypeGet
	CommandTypeAppend
)

type op struct {
	Type     commandType
	Key      string
	ClientID int
	ReqID    int64
	Data     string
}

type KVServer struct {
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	deadReq chan struct{}
	deadOk  chan struct{}

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	mu sync.Mutex
	// clientID -> reqID的映射, 防止client重复发送
	// 当op == nil时代表已经start进去了, 但是还没被apply, 需要等在channel上
	lastAppliedReq   map[int]op
	notification     map[int][]notifyStruct
	lastAppliedIndex int64
	data             map[string]string
}

type notifyStruct struct {
	term   int
	typ    string
	reqid  int64
	notify chan notify
}

type notify struct {
	NoLeader bool

	// useful for get notify
	Exists bool
	Value  string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	dPrintf("[server%v] Get command from clerk%v, reqid=%v", kv.me, args.ClientID, args.ReqID)
	kv.mu.Lock()
	idx, term, leader := kv.rf.Start(op{
		Type:     CommandTypeGet,
		Key:      args.Key,
		ClientID: args.ClientID,
		ReqID:    args.ReqID,
	})
	if !leader {
		dPrintf("[server%v] not leader, send ErrWrongLeader", kv.me)
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		return
	}

	dPrintf("[server%v] is leader, now waiting notify, log_index willbe=%v", kv.me, idx)

	notifyC := make(chan notify, 1)
	kv.notification[args.ClientID] = append(kv.notification[args.ClientID], notifyStruct{
		typ:    "Get",
		term:   term,
		notify: notifyC,
		reqid:  args.ReqID,
	})
	dPrintf("[server%v] notification is %v", kv.me, kv.notification)
	kv.mu.Unlock()

	n := <-notifyC
	dPrintf("[server%v] got notify %v, reqid=%v", kv.me, n, args.ReqID)
	if n.NoLeader {
		reply.Err = ErrWrongLeader
		return
	}
	if !n.Exists {
		reply.Err = ErrNoKey
		return
	}
	reply.Err = OK
	reply.Value = n.Value
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	dPrintf("[server%v] PutAppend is called", kv.me)
	kv.mu.Lock()
	var typ commandType
	if args.Op == "Append" {
		typ = CommandTypeAppend
	} else if args.Op == "Put" {
		typ = CommandTypePut
	} else {
		panic("checkme")
	}

	// 4 1
	idx, term, leader := kv.rf.Start(op{
		Type:     typ,
		Key:      args.Key,
		ClientID: args.ClientID,
		ReqID:    args.ReqID,
		Data:     args.Value,
	})
	if !leader {
		kv.mu.Unlock()
		dPrintf("[server%v] not leader, returning ErrWrongLeader", kv.me)
		reply.Err = ErrWrongLeader
		return
	}

	dPrintf("[server%v] is leader, now waiting notify, log_index willbe=%v", kv.me, idx)

	notifyC := make(chan notify, 1)
	kv.notification[args.ClientID] = append(kv.notification[args.ClientID], notifyStruct{
		typ:    args.Op,
		term:   term,
		notify: notifyC,
		reqid:  args.ReqID,
	})
	kv.mu.Unlock()

	n := <-notifyC
	if n.NoLeader {
		reply.Err = ErrWrongLeader
		return
	}
	reply.Err = OK
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	kv.deadReq <- struct{}{}
	<-kv.deadOk
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	dPrintf("server Start")

	labgob.Register(op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.notification = make(map[int][]notifyStruct)
	kv.lastAppliedReq = make(map[int]op)
	kv.data = make(map[string]string)
	kv.deadReq = make(chan struct{})
	kv.deadOk = make(chan struct{})

	go poller(kv)

	return kv
}

func poller(kv *KVServer) {
	for {
		select {
		case <-kv.deadReq:
			kv.mu.Lock()
			dPrintf("[server%v poller] be killed", kv.me)
			kv.rf.Kill()
			kv.mu.Unlock()
			kv.deadOk <- struct{}{}
			return
		case apply := <-kv.applyCh:
			dPrintf("[server%v poller] applyCh got %v", kv.me, apply)

			kv.mu.Lock()

			if apply.CommandValid {
				dPrintf("[server%v poller] got command apply", kv.me)

				opeartion := apply.Command.(op)

				// 保证幂等
				if opeartion.ReqID > kv.lastAppliedReq[opeartion.ClientID].ReqID {
					kv.lastAppliedReq[opeartion.ClientID] = opeartion
					switch opeartion.Type {
					case CommandTypeAppend:
						kv.data[opeartion.Key] = kv.data[opeartion.Key] + opeartion.Data
						dPrintf("got append now data is: %v", kv.data)
					case CommandTypeGet:
						// DO NOTHING
					case CommandTypePut:
						kv.data[opeartion.Key] = opeartion.Data
					}
				}

				// 做通知
				newNotify := make([]notifyStruct, 0)
				switch opeartion.Type {
				case CommandTypeAppend, CommandTypePut:
					for _, v := range kv.notification[opeartion.ClientID] {
						// if v.typ != "Put" && v.typ != "Append" {
						// 	continue
						// }
						if apply.CommandTerm != v.term {
							v.notify <- notify{
								NoLeader: true,
							}
						} else if v.reqid == opeartion.ReqID {
							v.notify <- notify{
								NoLeader: false,
							}
						} else {
							newNotify = append(newNotify, v)
						}
					}
					kv.notification[opeartion.ClientID] = newNotify
				case CommandTypeGet:
					for _, v := range kv.notification[opeartion.ClientID] {
						// if v.typ != "Get" {
						// 	continue
						// }
						if apply.CommandTerm != v.term {
							v.notify <- notify{
								NoLeader: true,
							}
						} else if v.reqid == opeartion.ReqID {
							value, ok := kv.data[opeartion.Key]
							v.notify <- notify{
								NoLeader: false,
								Exists:   ok,
								Value:    value,
							}
						} else {
							newNotify = append(newNotify, v)
						}
					}
					kv.notification[opeartion.ClientID] = newNotify
				}

				kv.lastAppliedIndex++

				// 此处检查raft state的size
				if kv.maxraftstate > 0 {
					if kv.rf.GetRaftStateSize() > int64(kv.maxraftstate) {
						dPrintf("[server%v poller] check state size too large(max=%v), snapshoting, size=%v",
							kv.me, kv.maxraftstate, kv.rf.GetRaftStateSize())
						buf := bytes.Buffer{}
						err := labgob.NewEncoder(&buf).Encode(kv.data)
						if err != nil {
							panic(err)
						}
						err = labgob.NewEncoder(&buf).Encode(kv.lastAppliedReq)
						if err != nil {
							panic(err)
						}

						dPrintf("[server%v poller] after encode, size=%v", kv.me, buf.Len())
						kv.rf.Snapshot(int(kv.lastAppliedIndex), buf.Bytes())
						dPrintf("[server%v poller] now snapshot length is %v", kv.me, kv.rf.GetRaftStateSize())
					}
				}
			} else {
				if !apply.SnapshotValid {
					panic("checkme")
				}

				dPrintf("[server%v poller] got snapshot apply", kv.me)
				// 此时应当应用这个snapshot, 然后通知所有等待方no leader
				dec := labgob.NewDecoder(bytes.NewReader(apply.Snapshot))
				err := dec.Decode(&kv.data)
				if err != nil {
					panic("unexpected")
				}
				err = dec.Decode(&kv.lastAppliedReq)
				if err != nil {
					panic("unexpected")
				}

				// 3
				// 1 2 3 0
				// 0 2 1
				// 一旦接收到snapshot就通知所有的等待方no leader
				for _, v := range kv.notification {
					for _, c := range v {
						c.notify <- notify{
							NoLeader: true,
						}
					}
				}
				kv.notification = make(map[int][]notifyStruct)
				kv.lastAppliedIndex = int64(apply.SnapshotIndex)
				dPrintf("[server%v poller] after got snapshot, now data is %v, lastAppliedIndex = %v", kv.me, kv.data, kv.lastAppliedIndex)
			}

			kv.mu.Unlock()
		}
	}
}
