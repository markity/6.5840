package raft

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
