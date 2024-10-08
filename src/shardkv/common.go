package shardkv

import (
	"os"

	"6.5840/logger"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const NShards = 10

const (
	OK              = "OK"
	ErrNoKey        = "ErrNoKey"
	ErrWrongGroup   = "ErrWrongGroup"
	ErrWrongLeader  = "ErrWrongLeader"
	ErrLeaderChange = "ErrLeaderChange"
	WaitForUpdate   = "WaitForUpdate"
	ErrInTransition = "InTransition"
)

var kvLogger = logger.NewLogger(logger.LL_TRACE, os.Stdout, "SHARD")

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
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
