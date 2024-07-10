package kvsrv

import (
	"crypto/rand"
	"math/big"
	"os"

	"6.5840/labrpc"
	"6.5840/logger"
)

var (
	topicClerk     = logger.LogTopic("CK.")
	idCounter  int = 0
)

type Clerk struct {
	server *labrpc.ClientEnd
	// You will have to modify this struct.
	xID TransactionID
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	// You'll have to add code here.
	ck.xID.UID = idCounter
	idCounter++
	ck.xID.Seqno = 0
	logger.Debug(topicClerk, "Clerk(%d) generated with id: %v\n", os.Getgid(), ck.xID.UID)
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	args := &GetArgs{Key: key, XID: ck.xID}
	reply := &GetReply{Value: ""}

	for {
		// Keep retrying until get the reply message
		if ck.server.Call("KVServer.Get", args, reply) {
			break
		}
	}

	// defer func() {
	// 	ck.xID.Seqno++
	// }()

	switch reply.Value {
	case "":
		return ""
	default:
		return reply.Value
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	// You will have to modify this function.
	args := &PutAppendArgs{Key: key, Value: value, XID: ck.xID}
	reply := &PutAppendReply{Value: ""}
	for {
		// Keep retrying until get the reply message
		if ck.server.Call("KVServer."+op, args, reply) {
			break
		}
	}
	ck.xID.Seqno++
	return reply.Value
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
