package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
	"6.5840/logger"
)

var idCounter int = 0

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	me          int
	leader      int
	nextOpSeqno int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func allocateClerkID() int {
	// 其实应该用一个锁
	idCounter++
	return idCounter - 1
}

func (ck *Clerk) allocateOpSeqno() int {
	ck.nextOpSeqno++
	return ck.nextOpSeqno - 1
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.me = allocateClerkID()
	ck.leader = 0
	ck.nextOpSeqno = 0
	scLogger.Debug(logger.LT_CtrlClerk, "%%%d: Clerk generated\n", ck.me)
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{ClerkId: ck.me, OpSeqno: ck.allocateOpSeqno(), Num: num}
	// Your code here.
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{ClerkId: ck.me, OpSeqno: ck.allocateOpSeqno(), Servers: servers}
	// Your code here.

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{ClerkId: ck.me, OpSeqno: ck.allocateOpSeqno(), GIDs: gids}
	// Your code here.

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{ClerkId: ck.me, OpSeqno: ck.allocateOpSeqno(), Shard: shard, GID: gid}
	// Your code here.

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
