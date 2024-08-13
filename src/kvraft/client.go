package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
	"6.5840/logger"
)

var idCounter int = 0

const retryIntervalMs = 100 * time.Millisecond

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
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
	// You'll have to add code here.
	ck.me = allocateClerkID()
	ck.leader = 0
	ck.nextOpSeqno = 0
	kvLogger.Debug(logger.LT_CLERK, "%%%d: Clerk generated\n", ck.me)
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	// rand.Int(rand.Reader, big.NewInt(int64(len(ck.servers))))
	args := GetArgs{Key: key, ClerkId: ck.me, OpSeqno: ck.allocateOpSeqno()}
	kvLogger.Trace(
		logger.LT_CLERK, "%%%d attempt to send %v<Seqno: %d, Key: %v> to %%%d\n",
		ck.me, "Get", args.OpSeqno, key, ck.leader,
	)

	// Clerk向Server发出RPC，如果无法连接，或Server不是Leader
	// 则不断尝试，直到顺利连接Leader
	// 如果Clerk无法连接到leader
	server := ck.leader
	for {
		reply := GetReply{}
		connected := false
		if ck.servers[server].Call("KVServer.Get", &args, &reply) { // 顺利连接到Server
			connected = true
			if reply.Err == OK { // 检查Server是否为Leader
				ck.leader = server
				kvLogger.Debug(
					logger.LT_CLERK,
					"%%%d: Successfuly executed %v<Seqno: %d, Key: %v> and got return value: %v\n",
					ck.me, "Get", args.OpSeqno, key, reply.Value,
				)
				return reply.Value
			}
		}
		serverPrev := server
		server = (server + 1) % len(ck.servers)
		kvLogger.Debug(
			logger.LT_CLERK,
			"%%%d: %v from %%%d failed(connected? %v), trying to connect %%%d\n",
			ck.me, "Get", serverPrev, connected, server,
		)
		time.Sleep(retryIntervalMs)
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{Key: key, Value: value, ClerkId: ck.me, OpSeqno: ck.allocateOpSeqno()}
	kvLogger.Trace(
		logger.LT_CLERK, "%%%d attempt to send %v<Seqno: %d, Key: %v, Value: %v> to %%%d\n",
		ck.me, op, args.OpSeqno, key, value, ck.leader,
	)

	// Clerk向Server发出RPC，如果无法连接，或Server不是Leader
	// 则不断尝试，直到顺利连接Leader
	// 如果Clerk无法连接到leader
	server := ck.leader
	for {
		// 避免labgob warning: Decoding into a non-default variable/field Err may not work
		reply := PutAppendReply{}
		connected := false
		if ck.servers[server].Call("KVServer."+op, &args, &reply) { // 顺利连接到Server
			connected = true
			if reply.Err == OK { // 检查Server是否为Leader
				ck.leader = server
				kvLogger.Debug(
					logger.LT_CLERK,
					"%%%d: Successfuly executed %v<Seqno: %d, Key: %v, Value: %v>\n",
					ck.me, op, args.OpSeqno, key, value,
				)
				return
			}
		}
		serverPrev := server
		server = (server + 1) % len(ck.servers)
		kvLogger.Debug(
			logger.LT_CLERK,
			"%%%d: %v from %%%d failed(connected? %v), trying to connect %%%d\n",
			ck.me, op, serverPrev, connected, server,
		)
		time.Sleep(retryIntervalMs)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
