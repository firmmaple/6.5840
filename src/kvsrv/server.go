package kvsrv

import (
	"log"
	"sync"

	"6.5840/logger"
)

var topicServer = logger.LogTopic("SERV")

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	data  map[string]string
	cache map[int]State
}

type State struct {
	seqno  int
	result []byte
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// It is both correct to return the old value or new value
  // (See Example7 in https://pdos.csail.mit.edu/6.824/notes/l-linearizability.txt)
	// Since using the old value need cache, and my implementation cann't pass
	// the TestMemGetManyClients, I simply exexute it again and return the new value

	// state, ok := kv.clientsState[args.XID.UID]
	// if !ok { // state haven't been added
	// 	state = State{seqno: -1}
	// }
	//
	// var value string
	// if state.seqno+1 == args.XID.Seqno {
	// 	value, ok = kv.data[args.Key]
	// 	if !ok {
	// 		value = ""
	// 	}
	// 	state.seqno = args.XID.Seqno
	// 	state.result = []byte(value)
	// 	kv.clientsState[args.XID.UID] = state
	// } else if state.seqno == args.XID.Seqno {
	// 	value = string(state.result)
	// } else {
	// 	logger.Fatal(topicServer, "Strange Seqno(GET): %v, %v", state.seqno, args.XID.Seqno)
	// }

	value, ok := kv.data[args.Key]
	if !ok {
		value = ""
	}
	reply.Value = value
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	state, ok := kv.cache[args.XID.UID]
	if !ok { // state haven't been added
		state = State{seqno: -1}
	}

	if state.seqno+1 == args.XID.Seqno {
		kv.data[args.Key] = args.Value
		state.seqno = args.XID.Seqno
		kv.cache[args.XID.UID] = state
	} else if state.seqno == args.XID.Seqno {
	} else {
		logger.Fatal(topicServer, "Strange Seqno(PUT): %v, %v", state.seqno, args.XID.Seqno)
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	var originalValue string

	state, ok := kv.cache[args.XID.UID]
	if !ok { // state haven't been added
		state = State{seqno: -1}
	}

	if state.seqno+1 == args.XID.Seqno {
		originalValue, ok = kv.data[args.Key]
		if !ok {
			originalValue = ""
		}
		kv.data[args.Key] = originalValue + args.Value

		state.seqno = args.XID.Seqno
		state.result = []byte(originalValue)
		kv.cache[args.XID.UID] = state
	} else if state.seqno == args.XID.Seqno {
		originalValue = string(state.result)
	} else {
		logger.Fatal(topicServer, "Strange Seqno(Append): %v, %v", state.seqno, args.XID.Seqno)
	}

	reply.Value = originalValue
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.data = make(map[string]string)
	kv.cache = make(map[int]State)
	return kv
}
