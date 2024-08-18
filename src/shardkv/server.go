package shardkv

import (
	"bytes"
	"fmt"
	"reflect"
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
)

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
	ClerkId int // 发起此操作的 Clerk 的 ID
	Seqno   int // 此操作在该 Clerk 中的序列号（从 0 开始）
	Shard   int
	Type    OpType
	Key     string
	Value   string
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
		"%v<Seqno: %d, Shard: %d, ClerkId: %d, Key: %v, Value: %v>",
		opType, op.Seqno, op.Shard, op.ClerkId, op.Key, op.Value,
	)
}

type identification struct {
	gid int
	me  int
}

func (id identification) String() string {
	return fmt.Sprintf("%%%d-%d", id.gid, id.me)
}

type Notifier struct {
	expectedSeqno int // 等待执行的opseqno
	cond          *sync.Cond
}

func (n Notifier) notify() {
	n.cond.Broadcast()
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
	config      shardctrler.Config
	dead        int32 // set by Kill()
	sm          *shardctrler.Clerk
	persister   *raft.Persister
	nextOpSeqno [NShards]map[int]int // 下一个待执行的Op序列号 shard -> (clerkid -> Seqno)
	notifiers   map[int]Notifier     // Op执行完毕后，通过对应的notifier通知Clerk
	data        map[string]string    // 存储KV键值对
}

func (kv *ShardKV) isLeader() bool {
	_, isLeader := kv.rf.GetState()
	return isLeader
}

func (kv *ShardKV) isExecuted(op *Op) bool {
	return op.Seqno < kv.nextOpSeqno[op.Shard][op.ClerkId]
}

// createSnapshot建一个快照，该快照包含直至commandIndex的所有状态变更
func (kv *ShardKV) createSnapshot(commandIndex int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(kv.nextOpSeqno) != nil || e.Encode(kv.data) != nil {
		kvLogger.Errorln(logger.LT_Persist, "Failed to encode some fields")
	}

	kv.rf.Snapshot(commandIndex, w.Bytes())
	kvLogger.Trace(
		logger.LT_Persist, "%v made a snapshot up to commandIndex %d\n",
		kv.id, commandIndex,
	)
}

// applySnapshot 加载并应用快照到当前状态，适用于以下场景：
//  1. server重启时调用applySnapshot，从已有的快照中恢复状态。
//  2. server调用createSnapshot创建快照后，收到Raft的ApplyMsg，
//     然后再调用applySnapshot，确认快照以生成，无任何状态变化
//  3. 当server的日志落后于leader时，leader发出InstallSnapshot更新快照，
//     raft将接受到的快照通过ApplyMsg传递给server，
//     然后通过server通过applySnapshot更新快照，实现server间的快照同步
func (kv *ShardKV) applySnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		kvLogger.Trace(logger.LT_Persist, "%v bootstrap without snapshot\n", kv.id)
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	if d.Decode(&kv.nextOpSeqno) != nil || d.Decode(&kv.data) != nil {
		kvLogger.Errorln(logger.LT_Persist, "Failed to decode some fields")
	}

	kvLogger.Trace(logger.LT_Persist, "%v recovered from snapshot\n", kv.id)
}

func (kv *ShardKV) submitOpAndWait(opPtr *Op) (Err, string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	_, _, isLeader := kv.rf.Start(*opPtr) // 将操作提交到 Raft，尝试在服务器间达成一致
	if !isLeader {
		return ErrWrongLeader, ""
	}
	kvLogger.Debug(
		logger.LT_SERVER, "%v handling %v from clerk %%%d\n", kv.id, opPtr, opPtr.ClerkId,
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

	return OK, kv.data[opPtr.Key]
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	opPtr := &Op{
		Seqno: args.OpSeqno, ClerkId: args.ClerkId, Shard: key2shard(args.Key), Type: OP_GET, Key: args.Key,
	}
	reply.Err = OK
	kv.mu.Lock()
	if kv.config.Shards[opPtr.Shard] != kv.gid {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	} else if kv.isExecuted(opPtr) {
		// Get第一次执行的时间 < 中间执行了0或多个命令 < 重复的Get到达时间
		// 所以尽管当前Server可能的数据可能是Stale的，但是直接返回仍然符合linearizable
		reply.Value = kv.data[args.Key]
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	reply.Err, reply.Value = kv.submitOpAndWait(opPtr)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	opType := OP_PUT
	if opType == OP_APPEND {
		opType = OP_APPEND
	}
	opPtr := &Op{
		Seqno: args.OpSeqno, ClerkId: args.ClerkId, Shard: key2shard(args.Key), Type: opType, Key: args.Key, Value: args.Value,
	}
	reply.Err = OK
	kv.mu.Lock()
	if kv.config.Shards[opPtr.Shard] != kv.gid {
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

func (kv *ShardKV) applyCommand(op Op, commandIndex int) {
	switch op.Type {
	case OP_GET: // GET 操作不修改数据，因此无需处理
	case OP_PUT:
		kv.data[op.Key] = op.Value
	case OP_APPEND:
		originalValue, ok := kv.data[op.Key]
		if !ok {
			originalValue = ""
		}
		kv.data[op.Key] = originalValue + op.Value
	default:
		kvLogger.Error(logger.LT_SERVER, "%v: Unknow Op Type %d\n", kv.id, op.Type)
	}
	kvLogger.Debug(logger.LT_SERVER, "%v executed OP %v\n", kv.id, op)
	if kv.nextOpSeqno[op.Shard][op.ClerkId] != op.Seqno { // 检查操作的序列号是否与预期一致，以防止操作顺序出错
		kvLogger.Error(
			logger.LT_SERVER, "%v: expected next op seqno of <%d, %%%d>: %d, but get %d \n",
			kv.id, op.Shard, op.ClerkId, kv.nextOpSeqno[op.Shard][op.ClerkId], op.Seqno,
		)
	}
	kv.nextOpSeqno[op.Shard][op.ClerkId]++ // 更新下一个预期执行的操作序列号
	kvLogger.Debug(
		logger.LT_SERVER, "%v updated nextOpSeqno of <%d, %%%d> to %d(CommandIndex: %v)\n",
		kv.id, op.Shard, op.ClerkId, kv.nextOpSeqno[op.Shard][op.ClerkId], commandIndex,
	)

	notification, ok := kv.notifiers[op.ClerkId]
	if ok && op.Seqno == notification.expectedSeqno {
		// 如果当前操作的序列号与预期的相符，则唤醒等待的 goroutine。
		// 这一步防止了因为旧操作未完成而提前唤醒等待的新操作的风险。
		// 具体而言，假设此刻Clerk %2请求执行Get命令(seqno 100)，server成功接受命令，
		// 但是在执行前Get命令前所有server都奔溃了。
		// server从崩溃恢复后会从头执行所有日志中的命令，然后问题就会出现，
		// 执行Clerk %2的第一个旧操作（seqno 0）时，会把等待Get命令(seqno 100)的Clerk 2唤醒，相当于提前执行的Get命令
		notification.notify()
	}
}

// executor 是 KVServer 的执行线程，它不断从 applyCh 中读取 Raft 的 ApplyMsg。
// 根据 ApplyMsg 的类型，executor 会执行对应的操作（如更新状态机或应用快照）。
func (kv *ShardKV) executor() {
	// Raft被Kill时会关闭applyCh，因此executor线程可以正常退出
	for applyMsg := range kv.applyCh {
		kv.mu.Lock()
		if applyMsg.SnapshotValid { // 如果收到的是有效的快照，则应用该快照来恢复状态
			kv.applySnapshot(applyMsg.Snapshot)
		} else if applyMsg.CommandValid { // 如果收到的是有效的命令，则尝试将命令转换为 Op 类型并执行
			op, ok := applyMsg.Command.(Op) // 尝试将 Command 断言为 Op 类型
			if !ok {
				actualType := reflect.TypeOf(applyMsg.Command)
				kvLogger.Error(
					logger.LT_SERVER, "%v: Command is not of type *Op, actual type: %v\n",
					kv.id, actualType,
				)
			}
			if op.Type == OP_NOOP || kv.isExecuted(&op) {
				// 如果是 NOOP 操作或者操作已经执行过，则不做任何处理
			} else {
				kv.applyCommand(op, applyMsg.CommandIndex)
			}
		}
		kv.mu.Unlock()
	}
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
			op := Op{Type: OP_NOOP}
			kv.rf.Start(op)
		}
		time.Sleep(noopTickIntervalMs * time.Millisecond)
	}
}

func (kv *ShardKV) getLatestConfig() {
	for !kv.killed() {
		latestConfig := kv.sm.Query(-1)
		kv.mu.Lock()
		kv.config = latestConfig
		kv.mu.Unlock()
		time.Sleep(1000 * time.Millisecond)
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
	kv.id = identification{gid: gid, me: me}
	kv.data = make(map[string]string)
	for i := 0; i < len(kv.nextOpSeqno); i++ {
		kv.nextOpSeqno[i] = make(map[int]int)
	}
	kv.notifiers = make(map[int]Notifier)

	// Use something like this to talk to the shardctrler:
	kv.sm = shardctrler.MakeClerk(kv.ctrlers)
	kv.persister = persister
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.getLatestConfig()
	go kv.executor()
	go kv.noopTicker()

	return kv
}
