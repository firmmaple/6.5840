package shardkv

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/logger"
	"6.5840/raft"
	"6.5840/shardctrler"
)

const (
	checkIsLeaderIntervalMs = 1000
	noopTickIntervalMs      = 1500
	snapshotRatio           = 0.9
	pollIntervialMS         = 10
)

type OpType int

const (
	OP_GET OpType = iota
	OP_PUT
	OP_APPEND
	OP_NOOP
	OP_START_CONFIG
	OP_END_CONFIG
	OP_INSTALL_SHARD
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClerkId     int // 发起此操作的 Clerk 的 ID
	Seqno       int // 此操作在该 Clerk 中的序列号（从 0 开始）
	Shard       int
	Type        OpType
	Key         string
	Value       string
	NewConfig   shardctrler.Config // OP_START_CONFIG
	ShardVers   int                // OP_INSTALL_SHARD
	ShardData   map[string]string  // OP_INSTALL_SHARD
	NextOpSeqno map[int]int        // OP_INSTALL_SHARD
}

func (op Op) String() string {
	opType, detail := "Unknow OpType", ""
	switch op.Type {
	case OP_GET:
		opType = "GET"
		detail = fmt.Sprintf("Key: %v", op.Key)
	case OP_PUT:
		opType = "PUT"
		detail = fmt.Sprintf("Key: %v, Value: %v", op.Key, op.Value)
	case OP_APPEND:
		opType = "Append"
		detail = fmt.Sprintf("Key: %v, Value: %v", op.Key, op.Value)
	case OP_NOOP:
		return "NoOp<>"
	case OP_START_CONFIG:
		return fmt.Sprintf(
			"StartConfig<ConfigNum: %d, Shards: %v>",
			op.NewConfig.Num, op.NewConfig.Shards,
		)
	case OP_END_CONFIG:
		opType = "EndConfig"
	case OP_INSTALL_SHARD:
		return fmt.Sprintf(
			"InstallShard<Shard: %v, Vers: %v>",
			op.Shard, op.ShardVers,
		)
	}

	return fmt.Sprintf(
		"%v<Seqno: %d, Shard: %d, ClerkId: %d, %v>", opType, op.Seqno, op.Shard, op.ClerkId, detail,
	)
}

type identification struct {
	Gid int
	Me  int
}

func (id identification) String() string {
	return fmt.Sprintf("%%%d-%d", id.Gid, id.Me)
}

type Notifier struct {
	expectedSeqno int // 等待执行的opseqno
	cond          *sync.Cond
}

func (n Notifier) notify() {
	n.cond.Broadcast()
}

type ShardDB struct {
	Db   map[string]string
	Vers int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	id          identification
	curConfig   shardctrler.Config
	nextConfig  shardctrler.Config
	dead        int32 // set by Kill()
	sm          *shardctrler.Clerk
	persister   *raft.Persister
	nextOpSeqno [NShards]map[int]int // 下一个待执行的Op序列号 shard -> (clerkid -> Seqno)
	notifiers   map[int]Notifier     // Op执行完毕后，通过对应的notifier通知Clerk
	// data          map[string]string    // 存储KV键值对
	shardDBs      [NShards]ShardDB
	servingShards []int
}

func (kv *ShardKV) isLeader() bool {
	_, isLeader := kv.rf.GetState()
	return isLeader
}

func (kv *ShardKV) isExecuted(op *Op) bool {
	if op.ClerkId == -1 {
		return false
	}
	return op.Seqno < kv.nextOpSeqno[op.Shard][op.ClerkId]
}

func (kv *ShardKV) isInTransition() bool {
	return kv.curConfig.Num+1 == kv.nextConfig.Num
}

func (kv *ShardKV) isTransitionFinished() bool {
	for _, shard := range kv.servingShards {
		if kv.shardDBs[shard].Vers != kv.nextConfig.Num {
			kvLogger.Info(
				logger.LT_Shard,
				"shard: %d,%v != %v\n",
				shard,
				kv.shardDBs[shard].Vers,
				kv.nextConfig.Num,
			)
			return false
		}
	}
	return true
}

// return true if op is GET, PUT or Append
func (kv *ShardKV) isClientComamnd(op *Op) bool {
	return op.ClerkId != -1
}

func (kv *ShardKV) submitOpAndWait(opPtr *Op) (Err, string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 将操作提交到 Raft，尝试在服务器间达成一致
	_, _, isLeader := kv.rf.Start(*opPtr)
	if !isLeader {
		return ErrWrongLeader, ""
	}
	kvLogger.Debug(
		logger.LT_SERVER, "%v handling %v from clerk %%%d\n",
		kv.id, opPtr, opPtr.ClerkId,
	)

	awakend := false // 用于标记操作是否成功提交并被唤醒
	notifier := Notifier{expectedSeqno: opPtr.Seqno, cond: sync.NewCond(&kv.mu)}
	kv.notifiers[opPtr.ClerkId] = notifier
	go func() {
		for { // 定期检查当前服务器是否仍然是 Leader，直到操作成功提交或失去领导权
			<-time.After(time.Duration(checkIsLeaderIntervalMs) * time.Millisecond)
			isLeader = kv.isLeader()
			if !isLeader || awakend {
				break
			}
		}
		if !isLeader {
			notifier.notify()
		}
	}()

	notifier.cond.Wait()
	awakend = true
	if !isLeader {
		kvLogger.Debug(logger.LT_SERVER, "%v: leadership changed\n", kv.id)
		return ErrLeaderChange, ""
	}

	return OK, kv.shardDBs[key2shard(opPtr.Key)].Db[opPtr.Key]
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	opPtr := &Op{
		Seqno: args.OpSeqno, ClerkId: args.ClerkId, Shard: key2shard(args.Key), Type: OP_GET, Key: args.Key,
	}
	reply.Err = OK
	kv.mu.Lock()
	if kv.isInTransition() {
		reply.Err = ErrInTransition
		kv.mu.Unlock()
		return
	} else if kv.curConfig.Shards[opPtr.Shard] != kv.gid {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	} else if kv.isExecuted(opPtr) {
		// Get第一次执行的时间 < 中间执行了0或多个命令 < 重复的Get到达时间
		// 所以尽管当前Server可能的数据可能是Stale的，但是直接返回仍然符合linearizable
		reply.Value = kv.shardDBs[key2shard(args.Key)].Db[args.Key]
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	reply.Err, reply.Value = kv.submitOpAndWait(opPtr)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	opType := OP_PUT
	if args.Op == "Append" {
		opType = OP_APPEND
	}
	opPtr := &Op{
		Seqno: args.OpSeqno, ClerkId: args.ClerkId, Shard: key2shard(args.Key), Type: opType, Key: args.Key, Value: args.Value,
	}
	reply.Err = OK
	kv.mu.Lock()
	if kv.isInTransition() {
		reply.Err = ErrInTransition
		kv.mu.Unlock()
		return
	} else if kv.curConfig.Shards[opPtr.Shard] != kv.gid {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	} else if kv.isExecuted(opPtr) {
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	reply.Err, _ = kv.submitOpAndWait(opPtr)
}

// noopTicker 定期向leader发送一个空的日志条目(noopTicker)
//
//  1. 场景设置:
//     系统中有四个服务器 A、B、C 和 D。
//     在 Term 4 时，A 是 Leader，并向 B 和 C 复制了日志条目 y（Entry y）。
//     此时，Entry y 在多数节点（A、B、C）上都有副本，但尚未提交（commit）。
//  2. D 断开连接并触发多次选举:
//     D 在断开连接期间进行了多次选举，导致它的任期号增加到了 10。此时，D 处于 Term 10。
//  3. D 重新连接并触发 Leader 降级:
//     当 D 重新连接到集群时，由于它的任期号更高（Term 10），A 必须将自己从Leader降级为Follower
//     （根据 Raft 的规则，发现更高任期的服务器时，Leader 自动降级为 Follower）。
//  4. A 在 Term 11 再次成为 Leader:
//     随后，在 Term 11 中，A 通过新的选举再次成为 Leader。
//  5. 日志提交的问题:
//     在 A 成为 Term 11 的 Leader 后，如果没有新的客户端命令，Leader A永远无法commit Entry y
//     尽管Entry y在Term 4中已经复制到了多数服务器，但由于Entry y是在之前的任期中创建的，
//     而 Raft 协议要求 Leader 在提交当前任期的日志条目之前，不能提交之前任期的日志条目。
//     因此，Entry y 不能被提交，也不能被应用到状态机中。
//
// 这时候我们就需要通过发送空条目，使Leader能顺利地提交所有之前任期的日志条目
func (kv *ShardKV) noopTicker() {
	for !kv.killed() {
		if kv.isLeader() {
			kv.mu.Lock()
			kvLogger.Debug(logger.LT_Shard, "%v intransition: %v\n", kv.id, kv.isInTransition())
			kv.mu.Unlock()
			op := Op{Type: OP_NOOP, ClerkId: -1}
			kv.rf.Start(op)
		}
		time.Sleep(noopTickIntervalMs * time.Millisecond)
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&kv.dead, 1)
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(
	servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int,
	gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd,
) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	// Your initialization code here.
	kv.id = identification{Gid: gid, Me: me}
	kv.nextConfig = kv.curConfig
	kv.notifiers = make(map[int]Notifier)
	kv.servingShards = make([]int, 0)
	for shard := 0; shard < NShards; shard++ {
		kv.shardDBs[shard].Db = make(map[string]string)
		kv.shardDBs[shard].Vers = 0
		kv.servingShards = append(kv.servingShards, shard)
		kv.nextOpSeqno[shard] = make(map[int]int)
	}

	// Use something like this to talk to the shardctrler:
	kv.sm = shardctrler.MakeClerk(kv.ctrlers)
	kv.persister = persister
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	if maxraftstate != -1 {
		kv.applySnapshot(persister.ReadSnapshot())
	}

	go kv.configPoller()
	go kv.executor()
	go kv.noopTicker()

	return kv
}
