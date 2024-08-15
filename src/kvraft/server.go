package kvraft

import (
	"bytes"
	"fmt"
	"log"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/logger"
	"6.5840/raft"
)

const (
	waitTimeoutInvervalMs = 1000
	noopTickIntervalMs    = 1500
	snapshotRatio         = 0.9
	Debug                 = false
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type OpType int

const (
	OP_GET OpType = iota
	OP_PUT
	OP_APPEND
	OP_NOOP
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Seqno   int
	ClerkId int
	Type    OpType
	Key     string
	Value   string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	persister            *raft.Persister
	nextOpSeqnoToExecute map[int]int
	wakeupNotifications  map[int]notification
	data                 map[string]string
}

type notification struct {
	expectedSeqno int // 等待执行的opseqno
	cond          *sync.Cond
}

func (n notification) notify() {
	n.cond.Broadcast()
}

func (op Op) String() string {
	var opType string
	switch op.Type {
	case OP_GET:
		opType = "GET"
	case OP_PUT:
		opType = "PUT"
	case OP_APPEND:
		opType = "Append"
	case OP_NOOP:
		opType = "NoOp"
	default:
		opType = "Unknow OpType"
	}
	return fmt.Sprintf(
		"%v<Seqno: %d, ClerkId: %d, Key: %v, Value: %v>",
		opType, op.Seqno, op.ClerkId, op.Key, op.Value,
	)
}

func (kv *KVServer) isLeader() bool {
	_, isLeader := kv.rf.GetState()
	return isLeader
}

func (kv *KVServer) isExecuted(op *Op) bool {
	return op.Seqno < kv.nextOpSeqnoToExecute[op.ClerkId]
}

func (kv *KVServer) makeSnapshot(commandIndex int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(kv.nextOpSeqnoToExecute) != nil || e.Encode(kv.data) != nil {
		kvLogger.Errorln(logger.LT_Persist, "Failed to encode some fields")
	}

	kv.rf.Snapshot(commandIndex, w.Bytes())
	kvLogger.Trace(
		logger.LT_Persist, "%%%d made a snapshot up to commandIndex %d\n",
		kv.me, commandIndex,
	)
}

func (kv *KVServer) recoverFromSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		kvLogger.Trace(logger.LT_Persist, "%%%d bootstrap without snapshot\n", kv.me)
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	if d.Decode(&kv.nextOpSeqnoToExecute) != nil || d.Decode(&kv.data) != nil {
		kvLogger.Errorln(logger.LT_Persist, "Failed to decode some fields")
	}

	kvLogger.Trace(logger.LT_Persist, "%%%d recovered from snapshot\n", kv.me)
}

func (kv *KVServer) waitUntilOpExecuted(opPtr *Op) (Err, string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	_, _, isLeader := kv.rf.Start(*opPtr)
	if !isLeader {
		return ErrWrongLeader, ""
	}

	awakend := false
	cond := sync.NewCond(&kv.mu)
	kv.wakeupNotifications[opPtr.ClerkId] = notification{expectedSeqno: opPtr.Seqno, cond: cond}
	go func() {
		for {
			<-time.After(time.Duration(waitTimeoutInvervalMs) * time.Millisecond)
			kv.mu.Lock()
			_, isLeader = kv.rf.GetState()
			if !isLeader || awakend {
				break
			}
			kv.mu.Unlock()
		}
		if !awakend {
			cond.Broadcast()
		}
		kv.mu.Unlock()
	}()

	cond.Wait()
	awakend = true
	if !isLeader {
		kvLogger.Debug(logger.LT_SERVER, "%%%d leader change\n", kv.me)
		return ErrLeaderChange, ""
	}

	// 好像不需要返回string？
	return OK, kv.data[opPtr.Key]
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	opPtr := &Op{Seqno: args.OpSeqno, ClerkId: args.ClerkId, Type: OP_GET, Key: args.Key}
	reply.Err = OK
	kv.mu.Lock()
	kvLogger.Debug(logger.LT_SERVER, "%%%d received Get RPC from clerk %%%d\n", kv.me, args.ClerkId)
	if kv.isExecuted(opPtr) {
		// 第一个Get执行时间 <= 最近执行一条命令的时间 < 这个Get到达时间
		// 所以尽管当前Server可能的数据可能是Stale的，但是直接返回仍然符合linearizable
		reply.Value = kv.data[args.Key]
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	reply.Err, reply.Value = kv.waitUntilOpExecuted(opPtr)
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// kvLogger.Debug(logger.LT_SERVER, "%%%d received Put RPC from clerk %%%d\n", kv.me, args.ClerkId)
	opPtr := &Op{
		Seqno: args.OpSeqno, ClerkId: args.ClerkId, Type: OP_PUT, Key: args.Key, Value: args.Value,
	}
	reply.Err = OK
	kv.mu.Lock()
	kvLogger.Debug(logger.LT_SERVER, "%%%d received Put RPC from clerk %%%d\n", kv.me, args.ClerkId)
	if kv.isExecuted(opPtr) {
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	reply.Err, _ = kv.waitUntilOpExecuted(opPtr)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	opPtr := &Op{
		Seqno: args.OpSeqno, ClerkId: args.ClerkId, Type: OP_APPEND, Key: args.Key, Value: args.Value,
	}
	reply.Err = OK
	kv.mu.Lock()
	kvLogger.Debug(
		logger.LT_SERVER, "%%%d received Append RPC from clerk %%%d\n",
		kv.me, args.ClerkId,
	)
	if kv.isExecuted(opPtr) {
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	reply.Err, _ = kv.waitUntilOpExecuted(opPtr)
}

func (kv *KVServer) executor() {
	// Raft被Kill时会关闭applyCh，因此executor线程可以正常退出
	for applyMsg := range kv.applyCh {
		kv.mu.Lock()
		if applyMsg.SnapshotValid {
			kv.recoverFromSnapshot(applyMsg.Snapshot)
			kv.mu.Unlock()
			continue
		}
		op, ok := applyMsg.Command.(Op) // 尝试将 Command 断言为 Op 类型
		if !ok {
			actualType := reflect.TypeOf(applyMsg.Command)
			kvLogger.Error(
				logger.LT_SERVER, "%%%d: Command is not of type *Op, actual type: %v\n",
				kv.me, actualType,
			)
		}
		if op.Type == OP_NOOP || kv.isExecuted(&op) {
			kv.mu.Unlock()
			continue
		}
		switch op.Type {
		case OP_GET: // 好像啥都不用做？
		case OP_PUT:
			kv.data[op.Key] = op.Value
		case OP_APPEND:
			originalValue, ok := kv.data[op.Key]
			if !ok {
				originalValue = ""
			}
			kv.data[op.Key] = originalValue + op.Value
		default:
			kvLogger.Error(logger.LT_SERVER, "%%%d: Unknow Op Type %d\n", kv.me, op.Type)
		}
		kvLogger.Debug(logger.LT_SERVER, "%%%d executed OP %v\n", kv.me, op)
		if kv.nextOpSeqnoToExecute[op.ClerkId] != op.Seqno {
			kvLogger.Error(
				logger.LT_SERVER, "%%%d: nextOpSeqno %d != op.Seqno %d \n",
				kv.me, kv.nextOpSeqnoToExecute[op.ClerkId], op.Seqno,
			)
		}
		kv.nextOpSeqnoToExecute[op.ClerkId]++
		kvLogger.Debug(
			logger.LT_SERVER, "%%%d updated nextOpSeqno of %%%d to %d(CommandIndex: %v)\n",
			kv.me, op.ClerkId, kv.nextOpSeqnoToExecute[op.ClerkId], applyMsg.CommandIndex,
		)

		if kv.maxraftstate != -1 &&
			float32(kv.persister.RaftStateSize()) > float32(kv.maxraftstate)*snapshotRatio {
			kv.makeSnapshot(applyMsg.CommandIndex)
		}

		notification, ok := kv.wakeupNotifications[op.ClerkId]
		if ok && op.Seqno == notification.expectedSeqno {
			// 如果没有expectedSeqno的话会有一个问题
			// 假设此刻Clerk %2发来一个Get命令(seqno 100)，但是在执行前Get命令前所有server都奔溃了
			// server恢复后会从头执行所有命令，然后问题就会出现，
			// 执行Clerk %2的第一个命令（seqno 0）时，会把等待Get命令的Clerk唤醒，相当于提前执行的Get命令
			notification.notify()
		}

		kv.mu.Unlock()
	}
}

func (kv *KVServer) noopTicker() {
	for !kv.killed() {
		if kv.isLeader() {
			op := Op{Type: OP_NOOP}
			kv.rf.Start(op)
		}
		time.Sleep(noopTickIntervalMs * time.Millisecond)
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(
	servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int,
) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.persister = persister
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.data = make(map[string]string)
	kv.nextOpSeqnoToExecute = make(map[int]int)
	kv.wakeupNotifications = make(map[int]notification)
	if maxraftstate != -1 {
		kv.recoverFromSnapshot(persister.ReadSnapshot())
	}

	go kv.executor()
	go kv.noopTicker()

	return kv
}
