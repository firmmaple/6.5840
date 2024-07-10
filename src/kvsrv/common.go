package kvsrv

type TransactionID struct {
	UID   int
	Seqno int
}

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	XID TransactionID
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	XID TransactionID
}

type GetReply struct {
	Value string
	// ackno int64
}
