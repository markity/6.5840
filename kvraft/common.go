package kvraft

const debug = true

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"

	// is only used in client
	ErrNetworkNotOK = "NetworkNotOK"
)

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"

	ClientID int
	ReqID    int64
}

type PutAppendReply struct {
	Err string
}

type GetArgs struct {
	Key string

	ClientID int
	ReqID    int64
}

type GetReply struct {
	Err   string
	Value string
}
