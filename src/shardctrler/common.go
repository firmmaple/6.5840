package shardctrler

import (
	"os"

	"6.5840/logger"
)

//
// Shard controller: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	OK              = "OK"
	ErrWrongLeader  = "ErrWrongLeader"
	ErrLeaderChange = "ErrLeaderChange"
)

// var scLogger = logger.NewLogger(logger.LL_TRACE, os.Stdout, "SHARD")
var scLogger = logger.NewLogger(logger.LL_DEBUG, os.Stdout, "SHARD")
// var scLogger = logger.NewLogger(logger.LL_INFO, os.Stdout, "SHARD")

type Err string

type JoinArgs struct {
	ClerkId int
	OpSeqno int
	Servers map[int][]string // new GID -> servers mappings
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	ClerkId int
	OpSeqno int
	GIDs    []int
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	ClerkId int
	OpSeqno int
	Shard   int
	GID     int
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	ClerkId int
	OpSeqno int
	Num     int // desired config number
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}
