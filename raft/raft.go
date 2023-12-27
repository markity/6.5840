package raft

// TODO: 2A, 真实的情况是服务器变为candidate后, 会再超时之前多次发RequestVote, 这需要多个timer
//		我需要实现这个

import (
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)

type Raft struct {
	// 对等端, 可以向它们同步信息
	peers []*labrpc.ClientEnd // RPC end points of all peers

	// 相当于一个wal, 能持久化数据, 能保证要么成功要么失败, 不会发生半写
	persister *Persister

	// 自己在peer的索引
	me int

	// 自己用channel做退出
	reqDead   chan struct{}
	reqDeadOK chan struct{}

	// 异步获取当前状态, 我写的代码是状态机, 最好不用锁, 不侵入状态机的状态, 通过请求打入状态机器循环
	reqGetState  chan struct{}
	getStateChan chan GetStateInfo

	reqGetRaftStateSizeChan chan struct{}
	getRaftStateSizeChan    chan int64

	// 发送命令的Channel, 以及反馈结果的管道
	sendCmdChan chan SendCmdChanInfo

	// 外部消息, 进入总线
	messagePipeLine chan Message

	snapshotChan chan DoSnapshotInfo

	// apply相关的设施
	applyCh    chan ApplyMsg
	applyQueue *unboundedQueue

	// 只有一个timer
	timer <-chan time.Time

	// 状态
	state RaftState
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.reqGetState <- struct{}{}
	s := <-rf.getStateChan

	return s.Term, s.Isleader
}

func (rf *Raft) GetRaftStateSize() int64 {
	rf.reqGetRaftStateSizeChan <- struct{}{}
	s := <-rf.getRaftStateSizeChan

	return s
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist(snapShot []byte) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(&rf.state.PersistInfo)
	if err != nil {
		panic(err)
	}
	raftstate := w.Bytes()
	if snapShot == nil {
		snapShot = rf.persister.ReadSnapshot()
	}
	rf.persister.Save(raftstate, snapShot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		rf.state.PersistInfo.Logs = make([]LogEntry, 1)
		rf.state.PersistInfo.Term = 0
		rf.state.PersistInfo.VotedForThisTerm = -1
		return
	}

	r := bytes.NewReader(data)
	d := labgob.NewDecoder(r)
	if err := d.Decode(&rf.state.PersistInfo); err != nil {
		panic(err)
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	ok := make(chan struct{})
	rf.snapshotChan <- DoSnapshotInfo{
		Index:          index,
		SnapShot:       snapshot,
		SnapshotOKChan: ok,
	}

	<-ok
}

func (rf *Raft) debug(format string, args ...interface{}) {
	if doDebug {
		s := fmt.Sprintf("%v(%v term=%v): ", rf.me, rf.state.State, rf.state.Term)
		s += format + "\n"
		log.Printf(s, args...)
	}
}

func (rf *Raft) debugForce(format string, args ...interface{}) {
	s := fmt.Sprintf("%v(%v term=%v): ", rf.me, rf.state.State, rf.state.Term)
	s += format + "\n"
	log.Printf(s, args...)
}

func (rf *Raft) randElectionTimer() {
	t := time.Millisecond*LEADER_ELECTION_TIMEOUT_LOW +
		time.Millisecond*time.Duration(
			(rand.Int()%(LEADER_ELECTION_TIMEOUT_HIGH-LEADER_ELECTION_TIMEOUT_LOW)))
	rf.debug("set rand election timer: %v", t)
	rf.timer = time.After(t)
}

func (rf *Raft) resetLeaderTimer() {
	rf.timer = time.After(time.Millisecond * HEARTBEAT_INTERVAL)
	rf.debug("reset leader timer")
}

func (rf *Raft) timerTimeout() {
	rf.timer = time.After(0)
	rf.debug("make timer timeout")
}

type empty struct{}

type requestVoteRequest struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type requestVoteReply struct {
	ReqTerm     int
	Term        int
	VoteGranted bool
}

// 心跳: AppendEntries RPCs that carry no log entries is heartbeat
type appendEntriesRequest struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PreLogTerm   int
	// may send more than one for efficiency
	// empty for heartbeat
	Entries      [][]byte
	LeaderCommit int
}

type appendEntriesReply struct {
	ID             int
	ReqTerm        int
	Term           int
	PreIndex       int
	Success        bool
	NLogsInRequest int
	// 实现简化版本的快速回退
	ConflictIndex int
}

type installSnapshotRequest struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Snapshot          []byte
}

type installSnapshotReply struct {
	ReqTerm              int
	ReqLastIncludedIndex int
	ReqLastIncludedTerm  int
	ID                   int
	Term                 int
}

// 外部调用, 会进入这里来
func (rf *Raft) RequestVoteReq(args *requestVoteRequest, reply *empty) {
	rf.messagePipeLine <- Message{
		Term: args.Term,
		Msg:  args,
	}
}

func (rf *Raft) RequestVoteReply(args *requestVoteReply, reply *empty) {
	rf.messagePipeLine <- Message{
		Term: args.Term,
		Msg:  args,
	}
}

func (rf *Raft) AppendEntries(args *appendEntriesRequest, reply *empty) {
	rf.messagePipeLine <- Message{
		Term: args.Term,
		Msg:  args,
	}
}

func (rf *Raft) AppendEntriesReply(args *appendEntriesReply, reply *empty) {
	rf.messagePipeLine <- Message{
		Term: args.Term,
		Msg:  args,
	}
}

func (rf *Raft) InstallSnapshot(args *installSnapshotRequest, reply *empty) {
	rf.messagePipeLine <- Message{
		Term: args.Term,
		Msg:  args,
	}
}

func (rf *Raft) InstallSnapshotReply(args *installSnapshotReply, reply *empty) {
	rf.messagePipeLine <- Message{
		Term: args.Term,
		Msg:  args,
	}
}

func (rf *Raft) commit(leaderCommitIndex int, newEntry LogEntry) {
	if leaderCommitIndex > rf.state.CommitIndex {
		oldCommitIndex := rf.state.CommitIndex
		rf.state.CommitIndex = min(leaderCommitIndex, newEntry.LogIndex)

		for i := oldCommitIndex + 1; i <= rf.state.CommitIndex; i++ {
			l, ok := rf.state.Logs.FindLogByIndex(i)
			if !ok {
				log.Panicf("oldCommitIndex=%v newCommitIndex=%v i = %v logs=%v", oldCommitIndex, rf.state.CommitIndex, i, rf.state.Logs)
				panic("checkme")
			}
			msg := ApplyMsg{
				CommandValid: true,
				Command:      l.Command,
				CommandIndex: i,
				CommandTerm:  newEntry.LogTerm,
			}
			rf.debug("applied log %v", msg)
			rf.applyQueue.Push(msg)
		}
	}
}

func (rf *Raft) leaderSendLogs(to int) {
	if rf.state.State != "leader" {
		panic("checkme")
	}

	if rf.state.NextLogIndex[to] <= rf.state.LastIncludedIndex {
		s := rf.persister.ReadSnapshot()
		rf.debug("leader send snapshot to %v, len(snapshot)=%v", to, len(s))
		go func(to int, term int, snapshot []byte, lastIncIndex int, lastIncTerm int) {
			rf.sendInstallSnapshotRequest(to, &installSnapshotRequest{
				Term:              term,
				LeaderID:          rf.me,
				LastIncludedIndex: lastIncIndex,
				LastIncludedTerm:  lastIncTerm,
				Snapshot:          snapshot,
			})
		}(to, rf.state.Term, s, rf.state.PersistInfo.LastIncludedIndex,
			rf.state.PersistInfo.LastIncludedTerm)
	} else {
		logsCopy_ := rf.state.Logs.Copy()
		nextIndexCopy_ := make([]int, len(rf.state.NextLogIndex))
		copy(nextIndexCopy_, rf.state.NextLogIndex)

		go func(i int, logs Logs, nextIndex []int, commitIndex int, term int) {
			/*
				If last log index ≥ nextIndex for a follower: send
				AppendEntries RPC with log entries starting at nextIndex
			*/
			lastLog := logs.LastLog()
			preLog, ok := logs.FindLogByIndex(nextIndex[i] - 1)
			if !ok {
				fmt.Println(logs, nextIndex)
				panic("checkme")
			}
			if lastLog.LogIndex >= nextIndex[i] {
				forPrint := make([]LogEntry, 0)
				tobeSendLogs := make([][]byte, 0)
				for i := nextIndex[i]; i <= lastLog.LogIndex; i++ {
					l, ok := logs.FindLogByIndex(i)
					if !ok {
						panic("checkme")
					}
					tobeSendLogs = append(tobeSendLogs, l.ToBytes())
					forPrint = append(forPrint, l)
				}
				if doDebug {
					log.Printf("%v send log(%v) to %v, prelog(index=%v term=%v)",
						rf.me, forPrint, i, preLog.LogIndex, preLog.LogTerm)
				}
				rf.sendAppendEntriesRequest(i, &appendEntriesRequest{
					Term:         term,
					LeaderID:     rf.me,
					PrevLogIndex: preLog.LogIndex,
					PreLogTerm:   preLog.LogTerm,
					Entries:      tobeSendLogs,
					LeaderCommit: commitIndex,
				})
			} else {
				if doDebug {
					log.Printf("%v send heartbeat to %v, prelog(index=%v term=%v)",
						rf.me, i, preLog.LogIndex, preLog.LogTerm)
				}
				rf.sendAppendEntriesRequest(i, &appendEntriesRequest{
					Term:         term,
					LeaderID:     rf.me,
					PrevLogIndex: lastLog.LogIndex,
					PreLogTerm:   lastLog.LogTerm,
					Entries:      nil,
					LeaderCommit: commitIndex,
				})
			}
		}(to, logsCopy_, nextIndexCopy_, rf.state.CommitIndex, rf.state.Term)
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVoteRequest(server int, args *requestVoteRequest) {
	rf.peers[server].Call("Raft.RequestVoteReq", args, &empty{})
}

func (rf *Raft) sendRequestVoteReply(server int, args *requestVoteReply) {
	rf.peers[server].Call("Raft.RequestVoteReply", args, &empty{})
}

func (rf *Raft) sendAppendEntriesRequest(server int, args *appendEntriesRequest) {
	rf.peers[server].Call("Raft.AppendEntries", args, &empty{})
}

func (rf *Raft) sendAppendEntriesReply(server int, args *appendEntriesReply) {
	rf.peers[server].Call("Raft.AppendEntriesReply", args, &empty{})
}

func (rf *Raft) sendInstallSnapshotRequest(server int, args *installSnapshotRequest) {
	rf.peers[server].Call("Raft.InstallSnapshot", args, &empty{})
}

func (rf *Raft) sendInstallSnapshotReply(server int, args *installSnapshotReply) {
	rf.peers[server].Call("Raft.InstallSnapshotReply", args, &empty{})
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (idx int, term int, isLeader bool) {
	// 防止send to channel panic

	c := make(chan SendCmdRespInfo)
	rf.sendCmdChan <- SendCmdChanInfo{
		Command: command,
		Resp:    c,
	}
	s := <-c
	return s.Index, s.Term, s.IsLeader
}

func (rf *Raft) Kill() {
	rf.reqDead <- struct{}{}
	<-rf.reqDeadOK
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.reqDead = make(chan struct{})
	rf.reqGetState = make(chan struct{})
	rf.getStateChan = make(chan GetStateInfo)
	rf.messagePipeLine = make(chan Message)
	rf.sendCmdChan = make(chan SendCmdChanInfo)
	rf.applyCh = applyCh
	rf.applyQueue = newUnboundedQueue()
	rf.reqDeadOK = make(chan struct{})
	rf.snapshotChan = make(chan DoSnapshotInfo)
	rf.getRaftStateSizeChan = make(chan int64)
	rf.reqGetRaftStateSizeChan = make(chan struct{})

	go func() {
		for {
			all := rf.applyQueue.PopAll()
			for _, v := range all {
				if doDebug {
					log.Printf("[async thread] %v send to applyCh %v\n", rf.me, v)
				}
				msg := v.(ApplyMsg)
				if msg.SnapshotValid {
					if doDebug {
						log.Printf("applyCh got snapshot: %s\n", string(msg.Snapshot))
					}
				}
				applyCh <- msg
			}
		}
	}()

	go stateMachine(rf)

	return rf
}

// 随机获得一个选举超时时间, 150~300ms
// 定时器的超时事件至少要和两次心跳间隔一样长
// broadcastTime ≪ electionTimeout ≪ MTBF

func stateMachine(rf *Raft) {
	p := PersistInfo{}
	labgob.NewDecoder(bytes.NewReader(rf.persister.ReadRaftState())).Decode(&p)
	log.Printf("%v read from disk comfirm2: %v\n", rf.me, p.LastIncludedIndex)

	rf.readPersist(rf.persister.ReadRaftState())
	if doDebug {
		log.Printf("%v Read From Persister: %#v\n", rf.me, rf.state.PersistInfo)
	}
	rf.state.State = "follower"
	rf.state.CandidateState = CandidateState{ReceivedNAgrees: 0}
	rf.state.LeaderState = LeaderState{
		NextLogIndex: make([]int, len(rf.peers)),
		MatchIndex:   make([]int, len(rf.peers)),
	}
	rf.debug("be follower")
	rf.randElectionTimer()
	snapShot := rf.persister.ReadSnapshot()
	if len(snapShot) != 0 {
		rf.debug("have initization snapshot, length=%v", len(snapShot))
		rf.state.CommitIndex = rf.state.LastIncludedIndex
		rf.applyQueue.Push(ApplyMsg{
			SnapshotValid: true,
			Snapshot:      rf.persister.ReadSnapshot(),
			SnapshotTerm:  rf.state.LastIncludedTerm,
			SnapshotIndex: rf.state.LastIncludedIndex,
		})
	}
	rf.debugForce("startup LasIncIndex=%v", rf.state.LastIncludedIndex)

	for {
		select {
		case <-rf.timer:
			rf.debug("timeout")
			switch rf.state.State {
			// 如果是follower超时, 那么进入candidate状态, 并且为自己加一票
			case "follower":
				rf.state.State = "candidate"
				rf.state.ReceivedNAgrees = 1
				rf.state.PersistInfo.VotedForThisTerm = rf.me
				rf.state.PersistInfo.Term++
				rf.persist(nil)

				rf.randElectionTimer()
				rf.debug("just timeout, being candidate, logs = %v", rf.state.Logs)

				// 并发地发送选票请求
				// 外部会共享这个变量, 为了并发安全我们需要拷贝一份t给协程用
				lastLog := rf.state.Logs.LastLog()
				for i := range rf.peers {
					if i != rf.me {
						go func(i int, t int, lastLog LogEntry) {
							rf.sendRequestVoteRequest(i, &requestVoteRequest{
								Term:         t,
								CandidateID:  rf.me,
								LastLogTerm:  lastLog.LogTerm,
								LastLogIndex: lastLog.LogIndex,
							})
						}(i, rf.state.Term, lastLog)
					}
				}
			// 说明candidate超时了, 加term继续
			case "candidate":
				rf.state.State = "candidate"
				rf.state.ReceivedNAgrees = 1
				rf.state.PersistInfo.Term++
				rf.persist(nil)
				rf.randElectionTimer()

				rf.debug("candidate timeout, retrying")

				// 外部会共享这个变量, 为了并发安全我们需要拷贝一份t给协程用
				for i := range rf.peers {
					if i != rf.me {
						go func(i int, t int, lastLog LogEntry) {
							rf.sendRequestVoteRequest(i, &requestVoteRequest{
								Term:         t,
								CandidateID:  rf.me,
								LastLogTerm:  lastLog.LogTerm,
								LastLogIndex: lastLog.LogIndex,
							})
						}(i, rf.state.Term, rf.state.Logs.LastLog())
					}
				}

			// leader超时是定时器超时, 只需要发送心跳维统治即可
			case "leader":
				rf.debug("timeout, logs=%v, nextIndex=%v, matchIndex=%v, lastIncIndex=%v, lastIncTerm=%v, commitIndex=%v", rf.state.Logs,
					rf.state.NextLogIndex, rf.state.MatchIndex, rf.state.LastIncludedIndex, rf.state.LastIncludedTerm, rf.state.CommitIndex)
				for i := range rf.peers {
					if i != rf.me {
						rf.leaderSendLogs(i)
					}
				}
				rf.resetLeaderTimer()
			}
		default:
			select {
			case <-rf.reqGetRaftStateSizeChan:
				sz := rf.persister.RaftStateSize()
				go func() {
					rf.getRaftStateSizeChan <- int64(sz)
				}()
			case info := <-rf.snapshotChan:
				rf.debug("got snapshot command, idx=%v logs=%v", info.Index, rf.state.Logs)

				// 收到裁减log的命令, 需要进行日志裁减, 然后把新的snapshot持久化
				l, ok := rf.state.Logs.FindLogByIndex(info.Index)
				if !ok {
					rf.debug("got snapshot command, but not found log in logs")
					info.SnapshotOKChan <- struct{}{}
					break
				}
				rf.state.Logs.TrimLogs(info.Index)
				rf.state.LastIncludedIndex = l.LogIndex
				rf.state.LastIncludedTerm = l.LogTerm
				rf.persist(info.SnapShot)
				rf.debugForce("got snapshot info: index=%v now logs=%v lastIncIdx=%v lastIncTerm=%v",
					info.Index, rf.state.Logs, rf.state.LastIncludedIndex, rf.state.LastIncludedTerm)

				info.SnapshotOKChan <- struct{}{}
			case <-rf.reqDead:
				rf.debug("dead")
				rf.reqDeadOK <- struct{}{}
				// 测试用例会在raft dead后发start, 需要避免问题我采取了个折中的方案
				go func() {
					for {
						c := <-rf.sendCmdChan
						c.Resp <- SendCmdRespInfo{Term: -1, Index: -1, IsLeader: false}
					}
				}()
				return
			case <-rf.reqGetState:
				rf.debug("rf.reqGetState")
				go func(t int, isLeader bool) {
					rf.getStateChan <- GetStateInfo{
						Term:     t,
						Isleader: isLeader,
					}
				}(rf.state.Term, rf.state.State == "leader")
			// 选举超时timer
			case <-rf.timer:
				rf.debug("timeout")
				switch rf.state.State {
				// 如果是follower超时, 那么进入candidate状态, 并且为自己加一票
				case "follower":
					rf.state.State = "candidate"
					rf.state.ReceivedNAgrees = 1
					rf.state.PersistInfo.VotedForThisTerm = rf.me
					rf.state.PersistInfo.Term++
					rf.persist(nil)

					rf.randElectionTimer()
					rf.debug("just timeout, being candidate, logs = %v", rf.state.Logs)

					// 并发地发送选票请求
					// 外部会共享这个变量, 为了并发安全我们需要拷贝一份t给协程用
					lastLog := rf.state.Logs.LastLog()
					for i := range rf.peers {
						if i != rf.me {
							go func(i int, t int, lastLog LogEntry) {
								rf.sendRequestVoteRequest(i, &requestVoteRequest{
									Term:         t,
									CandidateID:  rf.me,
									LastLogTerm:  lastLog.LogTerm,
									LastLogIndex: lastLog.LogIndex,
								})
							}(i, rf.state.Term, lastLog)
						}
					}
				// 说明candidate超时了, 加term继续
				case "candidate":
					rf.state.State = "candidate"
					rf.state.ReceivedNAgrees = 1
					rf.state.PersistInfo.Term++
					rf.persist(nil)
					rf.randElectionTimer()

					rf.debug("candidate timeout, retrying")

					// 外部会共享这个变量, 为了并发安全我们需要拷贝一份t给协程用
					for i := range rf.peers {
						if i != rf.me {
							go func(i int, t int, lastLog LogEntry) {
								rf.sendRequestVoteRequest(i, &requestVoteRequest{
									Term:         t,
									CandidateID:  rf.me,
									LastLogTerm:  lastLog.LogTerm,
									LastLogIndex: lastLog.LogIndex,
								})
							}(i, rf.state.Term, rf.state.Logs.LastLog())
						}
					}

				// leader超时是定时器超时, 只需要发送心跳维统治即可
				case "leader":
					rf.debug("timeout, logs=%v, nextIndex=%v, matchIndex=%v, lastIncIndex=%v, lastIncTerm=%v, commitIndex=%v", rf.state.Logs,
						rf.state.NextLogIndex, rf.state.MatchIndex, rf.state.LastIncludedIndex, rf.state.LastIncludedTerm, rf.state.CommitIndex)
					for i := range rf.peers {
						if i != rf.me {
							rf.leaderSendLogs(i)
						}
					}
					rf.resetLeaderTimer()
				}
			// 统一的外部事件总线, 从messagePipe进入
			case command := <-rf.sendCmdChan:
				rf.debug("send command %v", command.Command)
				switch rf.state.State {
				case "follower", "candidate":
					go func(t int) {
						command.Resp <- SendCmdRespInfo{
							Term:     t,
							Index:    -1,
							IsLeader: false,
						}
					}(rf.state.Term)
				case "leader":
					// 首先追加日志
					rf.state.Logs.Append(LogEntry{
						LogTerm:  rf.state.PersistInfo.Term,
						LogIndex: len(rf.state.PersistInfo.Logs),
						Command:  command.Command,
					})
					rf.persist(nil)
					l := rf.state.Logs.LastLog().LogIndex
					rf.debug("received command(%v), index would be %v, now logs is %v", command.Command, l, rf.state.Logs)
					go func(t int, l int) {
						command.Resp <- SendCmdRespInfo{
							Term:     t,
							Index:    l,
							IsLeader: true,
						}
					}(rf.state.PersistInfo.Term, l)

					// 为了尽快同步日志并返回客户端, 需要让定时器尽快过期
					rf.timerTimeout()
				}
			case input := <-rf.messagePipeLine:
				rf.debug("got message")
				/*
					if one server’s current
					term is smaller than the other’s, then it updates its current
					term to the larger value. If a candidate or leader discovers
					that its term is out of date, it immediately reverts to follower state.
					 If a server receives a request with a stale term
					number, it rejects the request.
				*/
				if rf.state.PersistInfo.Term < input.Term {
					rf.debug("found self term < remote term, turning into follower of term %v", input.Term)
					if rf.state.State == "leader" {
						rf.debug("leader be follower")
					}
					rf.state.State = "follower"
					rf.state.PersistInfo.Term = input.Term
					rf.state.PersistInfo.VotedForThisTerm = -1
					rf.persist(nil)

					/*
						If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)这个是不需要reset election timeout的
						注意修改currentTerm、votedFor、log[]其中一个后都要调用persist()方法，我之前就是因为第一个reply term那个点修改了currentTerm但是忘记调用了
						注意处理过期的RPC回复，student guide里面有写

						Make sure you reset your election timer exactly when Figure 2
						says you should. Specifically, you should only restart your election
						timer if a) you get an AppendEntries RPC from the current leader
						(i.e., if the term in the AppendEntries arguments is outdated, you
						should not reset your timer); b) you are starting an election; or c)
						you grant a vote to another peer.
					*/
					// timer = time.After(RandElectionTime())
				}

				switch val := input.Msg.(type) {
				case *installSnapshotRequest:
					rf.debug("got InstallSnapshotRequest, lastIncIdx=%v, lastIncTerm=%v, self logs=%v",
						val.LastIncludedIndex, val.LastIncludedTerm, rf.state.Logs)
					// 自己的term >= 对方了, 如果对面的term比自己小, 那么直接发信息让对方回到follower
					if val.Term < rf.state.Term {
						rf.debug("but self term(%v) > remote term(%v), sending to make it follower",
							rf.state.Term, val.Term)
						go func(t int) {
							rf.sendInstallSnapshotReply(val.LeaderID, &installSnapshotReply{
								ReqTerm: val.Term,
								Term:    t,
							})
						}(rf.state.Term)
						break
					}

					// 当前term相等, 是不可能产生两个leader的
					if rf.state.State == "leader" {
						panic("checkme")
					}

					if rf.state.State == "candidate" {
						rf.state.State = "follower"
						rf.state.PersistInfo.Term = input.Term
						rf.state.PersistInfo.VotedForThisTerm = rf.me
						rf.debug("got installSnapshotRequest from %v, candidate to follower(same term)", val.LeaderID)
						rf.persist(nil)
					}

					rf.randElectionTimer()

					/*
						If existing log entry has same index and term as snapshot’s
						last included entry, retain log entries following it and reply
					*/
					hasEntry, ok := rf.state.Logs.FindLogByIndex(val.LastIncludedIndex)
					if (ok && hasEntry.LogTerm == val.Term) ||
						rf.state.CommitIndex >= val.LastIncludedIndex {
						rf.debug("already has the entry, ignoring")
						go func(t int) {
							rf.sendInstallSnapshotReply(val.LeaderID, &installSnapshotReply{
								ReqTerm:              val.Term,
								ReqLastIncludedIndex: val.LastIncludedIndex,
								ReqLastIncludedTerm:  val.LastIncludedTerm,
								ID:                   rf.me,
								Term:                 t,
							})
						}(rf.state.Term)
						break
					}

					rf.debug("not have the entry, discarding all and putting it in applyCh, len(snapshot)=%v", len(val.Snapshot))
					// discard all logs
					var newLogs Logs
					newLogs = append(newLogs, LogEntry{
						LogTerm:  val.LastIncludedTerm,
						LogIndex: val.LastIncludedIndex,
						// it is for preLog, Command is useless
						Command: nil,
					})
					rf.state.Logs = newLogs
					rf.state.LastIncludedIndex = val.LastIncludedIndex
					rf.state.LastIncludedTerm = val.LastIncludedTerm
					rf.state.CommitIndex = val.LastIncludedIndex
					rf.persist(val.Snapshot)
					rf.applyQueue.Push(ApplyMsg{
						CommandValid:  false,
						SnapshotValid: true,
						Snapshot:      val.Snapshot,
						SnapshotTerm:  val.LastIncludedTerm,
						SnapshotIndex: val.LastIncludedIndex,
					})
					go func(t int) {
						rf.sendInstallSnapshotReply(val.LeaderID, &installSnapshotReply{
							ReqTerm:              val.Term,
							ReqLastIncludedIndex: val.LastIncludedIndex,
							ReqLastIncludedTerm:  val.LastIncludedTerm,
							ID:                   rf.me,
							Term:                 t,
						})
					}(rf.state.Term)
				case *installSnapshotReply:
					rf.debug("it is InstallSnapshotReply from %v", val.ID)

					if rf.state.State != "leader" {
						break
					}

					// 滞后的消息, 忽略
					if val.ReqTerm != rf.state.Term {
						break
					}

					// 来的消息是其它朝代的, 也许是自己之前当过leader, 然后reply滞后了
					//		这种情况不用管, 之后的心跳会同步它的
					if val.Term < rf.state.Term {
						break
					}

					nextLogMaybeSet := val.ReqLastIncludedIndex + 1
					rf.state.NextLogIndex[val.ID] = max(rf.state.NextLogIndex[val.ID], nextLogMaybeSet)
					rf.state.MatchIndex[val.ID] = rf.state.NextLogIndex[val.ID] - 1
					rf.debug("set server nextLogIndex=%v", rf.state.NextLogIndex[val.ID])

					if rf.state.NextLogIndex[val.ID] != rf.state.Logs.LastLog().LogIndex+1 {
						rf.debug("still have logs tobe send, sending selflogs=%v, nextindex=%v", rf.state.Logs, rf.state.NextLogIndex)
						rf.leaderSendLogs(val.ID)
					}

					sortedMatchIndex := make([]int, len(rf.state.MatchIndex))
					copy(sortedMatchIndex, rf.state.MatchIndex)
					sortedMatchIndex[rf.me] = len(rf.state.PersistInfo.Logs) - 1
					sort.Ints(sortedMatchIndex)
					N := sortedMatchIndex[len(rf.peers)/2]
					if N > rf.state.CommitIndex && rf.state.PersistInfo.Logs.
						GetByIndex(N).LogTerm == rf.state.PersistInfo.Term {

						oldCommitIndex := rf.state.CommitIndex
						rf.state.CommitIndex = N
						flag := false
						rf.debug("commit log, matchIdx=%v N=%v oldCommitIndex=%v, now is %v",
							rf.state.MatchIndex, N, oldCommitIndex, rf.state.CommitIndex)
						for i := oldCommitIndex + 1; i <= rf.state.CommitIndex; i++ {
							flag = true
							msg := ApplyMsg{
								CommandValid: true,
								Command:      rf.state.PersistInfo.Logs[i].Command,
								CommandIndex: i,
								CommandTerm:  rf.state.PersistInfo.Logs[i].LogTerm,
							}
							rf.debug("applied log: %v", msg)
							rf.applyQueue.Push(msg)
						}
						if !flag {
							panic("checkme")
						}
					}
				case *requestVoteReply:
					rf.debug("it is RequestVoteReply")
					// 如果是>当前term, 那么马上转变成follower, 更新term
					/*原文, 就算是VoteReply, 如果发现自己的term落后了, 也需要立马回到follower, 更新term
					All Servers
					If RPC request or response contains term T > currentTerm:
					set currentTerm = T, convert to follower (§5.1)
					*/

					if val.ReqTerm != rf.state.Term {
						break
					}

					// 如果是过去的消息, 直接无视
					if val.Term != rf.state.PersistInfo.Term {
						break
					}

					/*
						有两种情况:
						1. candidate竞争失败, 会退到当前term的follower
						2. 成为leader
					*/
					if rf.state.State != "candidate" {
						break
					}

					if val.VoteGranted {
						rf.state.ReceivedNAgrees++
						rf.debug("received vote ok, now have %v agreee votes", rf.state.ReceivedNAgrees)
						if rf.state.ReceivedNAgrees > len(rf.peers)/2 {
							rf.state.State = "leader"
							/*
								上任后, 认为每个节点都同步到了最新的日志
								for each server, index of the next log entry
								to send to that server (initialized to leader
								last log index + 1)
							*/
							nextIndex := rf.state.Logs.LastLogIndex() + 1
							for i := 0; i < len(rf.peers); i++ {
								rf.state.NextLogIndex[i] = nextIndex
								rf.state.MatchIndex[i] = 0
							}
							rf.debug("be leader, logs=%v, nextIndex=%v, matchIndex=%v", rf.state.Logs, rf.state.NextLogIndex, rf.state.MatchIndex)

							// 简便方法, 直接超时
							rf.timerTimeout()
						}
					}
				case *requestVoteRequest:
					rf.debug("received RequestVoteRequest from %v, remote last log is index=%v, term=%v, remote term=%v",
						val.CandidateID, val.LastLogIndex, val.LastLogTerm, val.Term)
					// 自己的term >= 对方的term, 作为candidate, 拒绝
					switch rf.state.State {
					case "candidate":
						// 自己的term >= val.Term, 那么直接拒绝这个, 给自己的term
						rf.debug("disgranted %v to be leader, remote term = %v", val.CandidateID, val.Term)
						go func(t int) {
							rf.sendRequestVoteReply(val.CandidateID, &requestVoteReply{
								Term:        t,
								VoteGranted: false,
								ReqTerm:     val.Term,
							})
						}(rf.state.Term)
					// 但是作为follower, 自己的term>=val.Term, 如果自己的term > 对方的term, 那么拒绝
					case "follower":
						/*
							If a server receives a request with a stale term
							number, it rejects the request.
						*/
						if rf.state.Term > val.Term {
							rf.debug("disgranted %v to be leader, remote term = %v < myterm",
								val.CandidateID, val.Term)
							go func(t int) {
								rf.sendRequestVoteReply(val.CandidateID, &requestVoteReply{
									Term:        t,
									VoteGranted: false,
									ReqTerm:     val.Term,
								})
							}(rf.state.Term)
						} else {
							// 这种情况下对面的term==自己的term, 如果自己没投票过, 那么就agree

							if rf.state.Term != val.Term {
								log.Panic("checkme")
							}

							if rf.state.PersistInfo.VotedForThisTerm == -1 {
								// 此外, 还需要管的是对方的日志比自己新
								lastLog := rf.state.Logs.LastLog()
								if val.LastLogTerm < lastLog.LogTerm || (val.LastLogTerm == lastLog.LogTerm &&
									val.LastLogIndex < lastLog.LogIndex) {
									rf.debug("disgranted %v to be leader beacuuse remote lastlog(term=%v index=%v) is older than my last log(term=%v index=%v)",
										val.CandidateID, val.LastLogTerm, val.LastLogIndex, lastLog.LogTerm, lastLog.LogIndex)
									go func(t int) {
										rf.sendRequestVoteReply(val.CandidateID, &requestVoteReply{
											Term:        t,
											VoteGranted: false,
											ReqTerm:     val.Term,
										})
									}(rf.state.Term)
								} else {
									rf.debug("granted %v to be leader, self logs=%v", val.CandidateID, rf.state.Logs)
									rf.state.PersistInfo.VotedForThisTerm = val.CandidateID
									rf.persist(nil)
									go func(t int) {
										rf.sendRequestVoteReply(val.CandidateID, &requestVoteReply{
											Term:        t,
											VoteGranted: true,
											ReqTerm:     val.Term,
										})
									}(rf.state.Term)

									// you should only restart your election timer if a)
									// you get an AppendEntries RPC from the current leader
									//  (i.e., if the term in the AppendEntries arguments is outdated,
									// you should not reset your timer); b) you are starting an election; or c)
									//  you grant a vote to another peer.
									rf.randElectionTimer()
								}
							} else {
								rf.debug("disgranted %v to be leader, ticket is already used to %v", val.CandidateID, rf.state.VotedForThisTerm)
								go func(t int) {
									rf.sendRequestVoteReply(val.CandidateID, &requestVoteReply{
										Term:        t,
										VoteGranted: false,
										ReqTerm:     val.Term,
									})
								}(rf.state.Term)
							}
						}
					case "leader":
						// 否则, 拒绝, 自己的term大于登于对方的term, 不能接受提议
						rf.debug("disgranted %v to be leader, because it is a leader which term >= remote", val.CandidateID)
						go func(t int) {
							rf.sendRequestVoteReply(val.CandidateID, &requestVoteReply{
								Term:        t,
								VoteGranted: false,
								ReqTerm:     val.Term,
							})
						}(rf.state.Term)
					}

				case *appendEntriesRequest:
					originPreLogIdx := val.PrevLogIndex
					// 进入这个case的时候self term >= remote term
					rf.debug("got ae from %v, remote term=%v, len of entries=%v, prelog(index=%v term=%v), self logs = %v",
						val.LeaderID, val.Term, len(val.Entries), val.PrevLogIndex, val.PreLogTerm, rf.state.Logs)
					entries := make([]LogEntry, 0)
					for _, v := range val.Entries {
						en, ok := BytesToLogEntry(v)
						if !ok {
							panic("checkme")
						}
						entries = append(entries, en)
					}
					rf.debug("entries = %v", entries)

					// 如果对方的term小于自己, 那么久直接拒绝日志, 对方收到term后会立刻回退到follower, 此时不用更新timer
					if rf.state.Term > val.Term {
						rf.debug("got AppendEntriesRequest from %v, but it's term(%v) < self, sending reply to make it a leader", val.LeaderID, val.Term)
						// 经测试, 必须包含ReqTerm才能保证正确性
						go func(t int) {
							rf.sendAppendEntriesReply(val.LeaderID, &appendEntriesReply{
								Term:    t,
								Success: false,
								ReqTerm: val.Term,
							})
						}(rf.state.Term)
						break
					}

					// 下面的逻辑是对面的term==自己的term了
					if rf.state.State == "leader" {
						panic("checkme")
					}

					if rf.state.State == "candidate" {
						rf.state.State = "follower"
						rf.state.PersistInfo.Term = input.Term
						rf.state.PersistInfo.VotedForThisTerm = rf.me
						rf.debug("got ae from %v, candidate to follower(same term)", val.LeaderID)
						rf.persist(nil)
					}

					// 重置timer
					rf.randElectionTimer()

					// 如果没有preLog的话直接拒绝日志, ConflictIndex = -1
					rf.debug("preLogIndex=%v", val.PrevLogIndex)
					// 1258 [1259 1260 1261 1262 1263 1264]
					// 1264
					if val.PrevLogIndex+len(entries) <= rf.state.CommitIndex {
						go func(t int) {
							rf.sendAppendEntriesReply(val.LeaderID, &appendEntriesReply{
								ID:             rf.me,
								Term:           t,
								PreIndex:       val.PrevLogIndex,
								Success:        true,
								NLogsInRequest: len(entries),
								ReqTerm:        val.Term,
							})
						}(rf.state.Term)
						break
					}

					if rf.state.Logs.LastLog().LogIndex < val.PrevLogIndex {
						go func(t int) {
							rf.sendAppendEntriesReply(val.LeaderID, &appendEntriesReply{
								ID:             rf.me,
								Term:           t,
								PreIndex:       val.PrevLogIndex,
								Success:        false,
								NLogsInRequest: len(val.Entries),
								ConflictIndex:  -1,
								ReqTerm:        val.Term,
							})
						}(rf.state.Term)
						break
					}

					// 1258 [1259 1260 1261]
					// 1260
					preLog, ok := rf.state.Logs.FindLogByIndex(val.PrevLogIndex)
					if !ok {
						fmt.Println(rf.state.LastIncludedIndex, val.PrevLogIndex)
						preLogNew := entries[rf.state.LastIncludedIndex-val.PrevLogIndex-1]
						entries = entries[rf.state.LastIncludedIndex-val.PrevLogIndex:]
						val.PrevLogIndex = preLogNew.LogIndex
						val.PreLogTerm = preLogNew.LogTerm
						preLog, ok = rf.state.Logs.FindLogByIndex(val.PrevLogIndex)
						if !ok {
							panic("checkme")
						}
					}

					// 如果已经拥有preLog, 那么需要检查是否应该丢弃log
					// preLog匹配不上, 要求leader回溯
					if preLog.LogTerm != val.PreLogTerm {
						conflictTerm := rf.state.Logs.GetByIndex(val.PrevLogIndex).LogTerm
						i := val.PrevLogIndex - rf.state.Logs[0].LogIndex
						for ; i > 0; i-- {
							if rf.state.Logs[i].LogTerm != conflictTerm {
								break
							}
						}
						rf.debug("prelog conflict, self logs = %v", rf.state.Logs)
						rf.state.Logs.TruncateBy(val.PrevLogIndex)
						rf.debug("prelog conflict, now logs = %v", rf.state.Logs)
						rf.persist(nil)
						go func(t int) {
							rf.sendAppendEntriesReply(val.LeaderID, &appendEntriesReply{
								ID:             rf.me,
								Term:           t,
								PreIndex:       val.PrevLogIndex,
								Success:        false,
								NLogsInRequest: len(val.Entries),
								ConflictIndex:  i + 1,
								ReqTerm:        val.Term,
							})
						}(rf.state.Term)
						break
					}

					rf.debug("prelog is matched")
					// preLog能匹配了, 如果Entries没有, 那么必然成功, 此时同步preLog那里
					if len(entries) == 0 {
						rf.commit(val.LeaderCommit, preLog)
					} else {
						// 此时entries是有很多日志的, 需要进行追加
						for _, entry := range entries {
							if checkSelfLog, ok := rf.state.Logs.FindLogByIndex(entry.LogIndex); ok {
								if checkSelfLog.LogTerm != entry.LogTerm {
									rf.debug("got log conflict, truncating")
									rf.state.Logs.TruncateBy(entry.LogIndex)
									rf.state.Logs.Append(entry)
								}
							} else {
								rf.state.Logs.Append(entry)
							}
						}
						rf.persist(nil)
						rf.commit(val.LeaderCommit, entries[len(entries)-1])
					}

					go func(t int) {
						rf.sendAppendEntriesReply(val.LeaderID, &appendEntriesReply{
							ID:             rf.me,
							Term:           t,
							PreIndex:       originPreLogIdx,
							Success:        true,
							NLogsInRequest: len(val.Entries),
							ReqTerm:        val.Term,
						})
					}(rf.state.Term)
				case *appendEntriesReply:
					rf.debug("it is AppendEntriesReply")
					// 此时自己的term>=对方的term
					// 只有leader理这个信息
					if rf.state.State != "leader" {
						break
					}

					if val.ReqTerm != rf.state.Term {
						break
					}

					// 来的消息是其它朝代的, 也许是自己之前当过leader, 然会reply滞后了
					//		这种情况不用管, 之后的心跳会同步它的
					if val.Term < rf.state.Term {
						break
					}

					// 如果已经在当前日志找不到前一个日志了, 就应当发送installSnapshot了, 我觉得下一次心跳发比较合适, 因为这可能是过时的信息
					preEntry, ok := rf.state.Logs.FindLogByIndex(rf.state.NextLogIndex[val.ID] - 1)
					if !ok {
						rf.debug("cannot find preLog, waiting heartbeat send snapshot")
						break
					}

					// 如果找到了前一个日志, 但是index不相等, 说明过时了, 直接忽略就行了
					if val.PreIndex != preEntry.LogIndex {
						break
					}

					if !val.Success {
						if val.ConflictIndex == -1 {
							l, ok := rf.state.Logs.FindLogByIndex(val.PreIndex)
							if !ok {
								panic("checkme")
							}
							// 5 6 7 8
							j := l.LogIndex - rf.state.Logs[0].LogIndex
							for j >= 0 && rf.state.Logs.At(j).LogTerm == l.LogTerm {
								j--
							}
							rf.debug("case2, j+1=%v", j+1)
							rf.state.NextLogIndex[val.ID] = min(rf.state.NextLogIndex[val.ID], rf.state.Logs.At(j+1).LogIndex)
						} else {
							rf.state.NextLogIndex[val.ID] = min(val.ConflictIndex, rf.state.NextLogIndex[val.ID])
							rf.debug("case2, ConflictIndex=%v", val.ConflictIndex)
							if rf.state.NextLogIndex[val.ID] == 0 {
								rf.debug("%v", val)
								panic("checkme")
							}
						}
						rf.debug("got refuse from %v, now nextIndex=%v", val.ID, rf.state.NextLogIndex[val.ID])
						rf.leaderSendLogs(val.ID)
						// success, 那么加nextIndex, 加matchIndex
					} else {
						nextLogMaybeSet := val.PreIndex + val.NLogsInRequest + 1
						rf.state.NextLogIndex[val.ID] = max(rf.state.NextLogIndex[val.ID], nextLogMaybeSet)
						rf.state.MatchIndex[val.ID] = rf.state.NextLogIndex[val.ID] - 1

						if rf.state.NextLogIndex[val.ID] != rf.state.Logs.LastLog().LogIndex+1 {
							rf.leaderSendLogs(val.ID)
						}
						rf.debug("set server %v nextIndex = %v", val.ID, rf.state.NextLogIndex[val.ID])

						// If there exists an N such that N > commitIndex,
						//  a majority of matchIndex[i] ≥ N, and log[N].
						// term == currentTerm: set commitIndex = N
						sortedMatchIndex := make([]int, len(rf.state.MatchIndex))
						copy(sortedMatchIndex, rf.state.MatchIndex)
						sortedMatchIndex[rf.me] = rf.state.Logs.LastLogIndex()
						sort.Ints(sortedMatchIndex)
						N := sortedMatchIndex[len(rf.peers)/2]
						rf.debug("leader checking can commit %v, N = %v", sortedMatchIndex, N)
						if N > rf.state.CommitIndex && rf.state.PersistInfo.Logs.
							GetByIndex(N).LogTerm == rf.state.PersistInfo.Term {

							oldCommitIndex := rf.state.CommitIndex
							rf.state.CommitIndex = N
							flag := false
							rf.debug("commit log, matchIdx=%v N=%v oldCommitIndex=%v, now is %v",
								rf.state.MatchIndex, N, oldCommitIndex, rf.state.CommitIndex)
							for i := oldCommitIndex + 1; i <= rf.state.CommitIndex; i++ {
								flag = true
								l, ok := rf.state.Logs.FindLogByIndex(i)
								if !ok {
									panic("checkme")
								}
								msg := ApplyMsg{
									CommandValid: true,
									Command:      l.Command,
									CommandIndex: i,
									CommandTerm:  l.LogTerm,
								}
								rf.debug("applied log: %v", msg)
								rf.applyQueue.Push(msg)
							}
							if !flag {
								panic("checkme")
							}
						}
					}
				}
			}
		}
	}
}
