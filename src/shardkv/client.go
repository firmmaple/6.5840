package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
	"6.5840/logger"
	"6.5840/shardctrler"
)

var idCounter int = 0

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	me          int
	leader      int
	nextOpSeqno []int
}

func allocateClerkID() int {
	idCounter++
	return idCounter - 1
}

func (ck *Clerk) allocateOpSeqno(shard int) int {
	ck.nextOpSeqno[shard]++
	return ck.nextOpSeqno[shard] - 1
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.me = allocateClerkID()
	ck.leader = 0
	ck.nextOpSeqno = make([]int, NShards)
	kvLogger.Debug(logger.LT_CLERK, "%%%d: Clerk generated\n", ck.me)
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *Clerk) Get(key string) string {
	shard := key2shard(key)
	args := GetArgs{Key: key, ClerkId: ck.me, OpSeqno: ck.allocateOpSeqno(shard)}

	for {
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply GetReply
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}
				kvLogger.Trace(
					logger.LT_CLERK,
					"%%%d: sent to %v failed(ok: %v, err: %v)\n",
					ck.me, servers[si], ok, reply.Err,
				)
				if ok && reply.Err == ErrInTransition {
					si -= 1
					time.Sleep(100 * time.Millisecond)
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controller for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}

	return ""
}

// shared by Put and Append.
// You will have to modify this function.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	shard := key2shard(key)
	args := PutAppendArgs{
		Key: key, Value: value, Op: op, ClerkId: ck.me, OpSeqno: ck.allocateOpSeqno(shard),
	}

	for {
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				kvLogger.Trace(
					logger.LT_CLERK,
					"%%%v attempt to %v<Key %v(shard %d), Value: %v> to %v\n",
					ck.me, op, key, shard, value, servers[si],
				)
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok && reply.Err == OK {
					return
				}
				if ok && reply.Err == ErrWrongGroup {
					break
				}
				kvLogger.Trace(
					logger.LT_CLERK,
					"%%%d: sent to %v failed(ok: %v, err: %v)\n",
					ck.me, servers[si], ok, reply.Err,
				)
				if ok && reply.Err == ErrInTransition {
					si -= 1
					time.Sleep(100 * time.Millisecond)
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controller for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
