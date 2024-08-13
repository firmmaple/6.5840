package kvraft

import (
	"os"

	"6.5840/logger"
)

var kvLogger = logger.NewLogger(logger.LL_TRACE, os.Stdout, "KV")

const (
	OK              = "OK"
	ErrNoKey        = "ErrNoKey"
	ErrWrongLeader  = "ErrWrongLeader"
	ErrLeaderChange = "ErrLeaderChange"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClerkId int
	OpSeqno int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClerkId int
	OpSeqno int
}

type GetReply struct {
	Err   Err
	Value string
}
