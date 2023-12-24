package raft

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type GetStateInfo struct {
	Term     int
	Isleader bool
}

type DoSnapshotInfo struct {
	Index    int
	SnapShot []byte

	SnapshotOKChan chan struct{}
}

type Message struct {
	Term int
	Msg  interface{}
}

type SendCmdChanInfo struct {
	Command interface{}
	Resp    chan SendCmdRespInfo
}

type SendCmdRespInfo struct {
	Term     int
	Index    int
	IsLeader bool
}

type RaftState struct {
	State       string
	CommitIndex int

	FollowerState
	CandidateState
	LeaderState
	PersistInfo
}

// 初始为follower, follower在某个term只能投票一次
type FollowerState struct {
}

// 候选者, 当follower超时后, 递增term并进入这个状态, 在开始选举的时候发一次
// request vote请求, 实际实现是在进入candidate状态后会发送多次request vote, 但是
// 我简化了步骤, 只发一次
// TODO: 实现超时前多次发送
type CandidateState struct {
	ReceivedNAgrees int
}

type LeaderState struct {
	// 存储每个节点上个日志的日志索引
	NextLogIndex []int
	MatchIndex   []int
}

type PersistInfo struct {
	Term              int
	Logs              Logs
	VotedForThisTerm  int
	LastIncludedIndex int
	LastIncludedTerm  int
}
