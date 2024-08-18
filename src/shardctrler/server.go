package shardctrler

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/logger"
	"6.5840/raft"
)

var (
	checkIsLeaderIntervalMs = 1000
	noopTickIntervalMs      = 1500
)

type OpType int

const (
	OP_JOIN OpType = iota
	OP_LEAVE
	OP_MOVE
	OP_QUERY
	OP_NOOP
)

type Op struct {
	// Your data here.
	ClerkId int // 发起此操作的 Clerk 的 ID
	Seqno   int // 此操作在该 Clerk 中的序列号（从 0 开始）
	Type    OpType
	Servers map[int][]string // Join
	GIDs    []int            // Leave
	Shard   int              // Move
	GID     int              // Move
	Num     int              // Query
}

func (op Op) String() string {
	opType, detail := "Unknow OpType", ""
	switch op.Type {
	case OP_JOIN:
		opType = "Join"
		detail = fmt.Sprintf("Servers: %v", op.Servers)
	case OP_LEAVE:
		opType = "Leave"
		detail = fmt.Sprintf("GIDs: %v", op.GIDs)
	case OP_MOVE:
		opType = "Move"
		detail = fmt.Sprintf("Shard: %v -> GID: %v", op.Shard, op.GID)
	case OP_QUERY:
		opType = "Query"
		detail = fmt.Sprintf("Num: %v", op.Num)
	case OP_NOOP:
		opType = "NoOp"
	}
	return fmt.Sprintf(
		"%v<Seqno: %d, ClerkId: %d, %v>", opType, op.Seqno, op.ClerkId, detail,
	)
}

type groupShardInfo struct {
	gid            int
	shards         []int
	idealShardsNum int
}

type groupShardInfos []*groupShardInfo

func (infos groupShardInfos) String() string {
	entries := make([]string, len(infos))
	for i, info := range infos {
		entries[i] = fmt.Sprintf(
			"\tGroup %d - IdealShardNum: %d, CurrentShardsNum: %d, Shards: %v",
			info.gid, info.idealShardsNum, len(info.shards), info.shards,
		)
	}
	return fmt.Sprintf("\n%v", strings.Join(entries, "\n"))
}

type Notifier struct {
	expectedSeqno int // 等待执行的opseqno
	cond          *sync.Cond
}

func (n Notifier) notify() {
	n.cond.Broadcast()
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	dead        int32
	nextOpSeqno map[int]int      // 下一个待执行的Op序列号
	notifiers   map[int]Notifier // Op执行完毕后，通过对应的notifier通知Clerk
	configs     []Config         // indexed by config num
}

func (sc *ShardCtrler) isLeader() bool {
	_, isLeader := sc.rf.GetState()
	return isLeader
}

func (sc *ShardCtrler) isExecuted(op *Op) bool {
	return op.Seqno < sc.nextOpSeqno[op.ClerkId]
}

func (sc *ShardCtrler) lastConfig() Config {
	return sc.configs[len(sc.configs)-1]
}

func (sc *ShardCtrler) cloneLastConfig() Config {
	lastConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{
		Num:    lastConfig.Num,
		Shards: lastConfig.Shards, // 数组是值类型，因此可以直接赋值
		Groups: make(map[int][]string),
	}
	for gid, servers := range lastConfig.Groups {
		newServers := make([]string, len(servers))
		copy(newServers, servers)
		newConfig.Groups[gid] = newServers
	}
	return newConfig
}

func (sc *ShardCtrler) rebalance(newConfig *Config) {
	gid2shards := make(map[int][]int)
	for gid := range newConfig.Groups {
		gid2shards[gid] = make([]int, 0)
		for shard, assignedGid := range newConfig.Shards {
			if gid == assignedGid {
				gid2shards[gid] = append(gid2shards[gid], shard)
			}
		}
	}

	infos := make(groupShardInfos, 0)
	for gid, shards := range gid2shards {
		info := groupShardInfo{gid: gid, shards: shards}
		sort.Ints(info.shards)
		infos = append(infos, &info)
	}
	sort.Slice(infos, func(i, j int) bool {
		return infos[i].gid < infos[j].gid
	})
	numGroups := len(gid2shards)
	if numGroups == 0 {
		return
	}
	baseShardsNum := NShards / numGroups
	reminderShardsNum := NShards % numGroups
	// fmt.Printf("base: %d, reminder: %d\n", baseShardsNum, reminderShardsNum)

	overAssignedGroups := make(groupShardInfos, 0)
	underAssignedGroups := make(groupShardInfos, 0)
	for i, info := range infos {
		info.idealShardsNum = baseShardsNum
		if i < reminderShardsNum {
			info.idealShardsNum++
		}
		if len(info.shards) < info.idealShardsNum {
			underAssignedGroups = append(underAssignedGroups, info)
		} else if len(info.shards) > info.idealShardsNum {
			overAssignedGroups = append(overAssignedGroups, info)
		}
	}

	scLogger.Trace(
		logger.LT_Rebalance,
		"%%%d: before rebalance groupShardInfos %v\n", sc.me, infos,
	)

	pendingShards := make([]int, 0)
	for _, overAssigned := range overAssignedGroups {
		length := len(overAssigned.shards)
		gap := length - overAssigned.idealShardsNum
		pendingShards = append(pendingShards, overAssigned.shards[length-gap:]...)
		overAssigned.shards = overAssigned.shards[:length-gap] // 用于之后打印infos
	}
	for shard := range newConfig.Shards {
		if newConfig.Shards[shard] == 0 {
			pendingShards = append(pendingShards, shard)
		}
	}

	sort.Ints(pendingShards)
	scLogger.Trace(
		logger.LT_Rebalance,
		"%%%d: during rebalance pendingShards %v\n", sc.me, pendingShards,
	)

	nextShardIndex := 0
	for _, underAssigned := range underAssignedGroups {
		gap := underAssigned.idealShardsNum - len(underAssigned.shards)
		endIndex := nextShardIndex + gap
		for ; nextShardIndex < endIndex; nextShardIndex++ {
			nextShard := pendingShards[nextShardIndex]
			newConfig.Shards[nextShard] = underAssigned.gid
			underAssigned.shards = append(underAssigned.shards, nextShard) // 无意义，只是由于之后打印infos
		}
	}
	scLogger.Info(logger.LT_Rebalance, "%%%d: after rebalance groupShardInfos %v\n", sc.me, infos)
}

func (kv *ShardCtrler) createSnapshot(commandIndex int) {
}

func (kv *ShardCtrler) applySnapshot(snapshot []byte) {
}

func (sc *ShardCtrler) submitOpAndWait(opPtr *Op) (bool, Err) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	_, _, isLeader := sc.rf.Start(*opPtr) // 将操作提交到 Raft，尝试在服务器间达成一致
	if !isLeader {
		return true, ErrWrongLeader
	}
	scLogger.Debug(
		logger.LT_CtrlServer, "%%%d handling %v from clerk %%%d\n",
		sc.me, opPtr, opPtr.ClerkId,
	)

	awakend := false // 用于标记操作是否成功提交并被唤醒
	notifier := Notifier{expectedSeqno: opPtr.Seqno, cond: sync.NewCond(&sc.mu)}
	sc.notifiers[opPtr.ClerkId] = notifier
	go func() {
		for { // 定期检查当前服务器是否仍然是 Leader，直到操作成功提交或失去领导权
			<-time.After(time.Duration(checkIsLeaderIntervalMs) * time.Millisecond)
			isLeader = sc.isLeader()
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
		scLogger.Debug(logger.LT_CtrlServer, "%%%d: leadership changed\n", sc.me)
		return true, ErrLeaderChange
	}

	// return OK, sc.data[opPtr.Key]
	return false, ""
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := &Op{
		ClerkId: args.ClerkId, Seqno: args.OpSeqno, Type: OP_JOIN, Servers: args.Servers,
	}
	reply.Err = OK
	sc.mu.Lock()
	// scLogger.Debug(
	// 	logger.LT_CtrlServer, "%%%d received %v from clerk %%%d\n",
	// 	sc.me, op, args.ClerkId,
	// )
	if sc.isExecuted(op) {
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	reply.WrongLeader, reply.Err = sc.submitOpAndWait(op)
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := &Op{
		ClerkId: args.ClerkId, Seqno: args.OpSeqno, Type: OP_LEAVE, GIDs: args.GIDs,
	}
	reply.Err = OK
	sc.mu.Lock()
	// scLogger.Debug(
	// 	logger.LT_CtrlServer, "%%%d received %v from clerk %%%d\n",
	// 	sc.me, op, args.ClerkId,
	// )
	if sc.isExecuted(op) {
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	reply.WrongLeader, reply.Err = sc.submitOpAndWait(op)
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := &Op{
		ClerkId: args.ClerkId, Seqno: args.OpSeqno, Type: OP_MOVE, Shard: args.Shard, GID: args.GID,
	}
	reply.Err = OK
	sc.mu.Lock()
	// scLogger.Debug(
	// 	logger.LT_CtrlServer, "%%%d received %v from clerk %%%d\n",
	// 	sc.me, op, args.ClerkId,
	// )
	if sc.isExecuted(op) {
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	reply.WrongLeader, reply.Err = sc.submitOpAndWait(op)
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := &Op{
		ClerkId: args.ClerkId, Seqno: args.OpSeqno, Type: OP_QUERY, Num: args.Num,
	}
	reply.Err = OK
	sc.mu.Lock()
	// scLogger.Debug(
	// 	logger.LT_CtrlServer, "%%%d received %v from clerk %%%d\n",
	// 	sc.me, op, args.ClerkId,
	// )
	if sc.isExecuted(op) {
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	reply.WrongLeader, reply.Err = sc.submitOpAndWait(op)
	sc.mu.Lock()
	if args.Num < 0 || args.Num > len(sc.configs) {
		args.Num = len(sc.configs) - 1
	}
	reply.Config = sc.configs[args.Num]
	sc.mu.Unlock()
}

// applyCommand 执行指定的操作（op），并在操作执行后通知相关的 goroutine 该操作已完成
func (sc *ShardCtrler) applyCommand(op Op, commandIndex int) {
	switch op.Type {
	case OP_JOIN:
		newConfig := sc.cloneLastConfig()
		newConfig.Num++
		for gid, servers := range op.Servers {
			if _, isExisted := newConfig.Groups[gid]; isExisted {
				scLogger.Warn(
					logger.LT_CtrlServer,
					"%%%d gourp %d is alreay in configuration\n", sc.me, gid,
				)
				continue
			}
			newConfig.Groups[gid] = servers
		}
		sc.rebalance(&newConfig)
		sc.configs = append(sc.configs, newConfig)
	case OP_LEAVE:
		newConfig := sc.cloneLastConfig()
		newConfig.Num++
		for _, gid := range op.GIDs {
			if _, isExisted := newConfig.Groups[gid]; !isExisted {
				scLogger.Warn(
					logger.LT_CtrlServer,
					"%%%d gourp %d is alreay not in configuration\n", sc.me, gid,
				)
				continue
			}
			for shard := range newConfig.Shards {
				if newConfig.Shards[shard] == gid {
					newConfig.Shards[shard] = 0
				}
			}
			delete(newConfig.Groups, gid)
		}
		sc.rebalance(&newConfig)
		sc.configs = append(sc.configs, newConfig)
	case OP_MOVE:
		newConfig := sc.cloneLastConfig()
		newConfig.Num++
		newConfig.Shards[op.Shard] = op.GID
		sc.configs = append(sc.configs, newConfig)
	case OP_QUERY: // Query 不修改数据，因此无需处理
	default:
		scLogger.Error(logger.LT_CtrlServer, "%%%d: Unknow Op Type %d\n", sc.me, op.Type)
	}
	scLogger.Info(logger.LT_CtrlServer, "%%%d executed %v\n", sc.me, op)
	if sc.nextOpSeqno[op.ClerkId] != op.Seqno { // 检查操作的序列号是否与预期一致，以防止操作顺序出错
		scLogger.Error(
			logger.LT_CtrlServer, "%%%d: nextOpSeqno %d != op.Seqno %d \n",
			sc.me, sc.nextOpSeqno[op.ClerkId], op.Seqno,
		)
	}
	sc.nextOpSeqno[op.ClerkId]++ // 更新下一个预期执行的操作序列号
	scLogger.Debug(
		logger.LT_CtrlServer, "%%%d updated nextOpSeqno of %%%d to %d(CommandIndex: %v)\n",
		sc.me, op.ClerkId, sc.nextOpSeqno[op.ClerkId], commandIndex,
	)

	notification, ok := sc.notifiers[op.ClerkId]
	if ok && op.Seqno == notification.expectedSeqno {
		// 如果当前操作的序列号与预期的相符，则唤醒等待的 goroutine。
		// 这一步防止了因为旧操作未完成而提前唤醒等待的新操作的风险。
		notification.notify()
	}
}

// executor 是 KVServer 的执行线程，它不断从 applyCh 中读取 Raft 的 ApplyMsg。
// 根据 ApplyMsg 的类型，executor 会执行对应的操作（如更新状态机或应用快照）。
func (sc *ShardCtrler) executor() {
	// Raft被Kill时会关闭applyCh，因此executor线程可以正常退出
	for applyMsg := range sc.applyCh {
		sc.mu.Lock()
		if applyMsg.SnapshotValid { // 如果收到的是有效的快照，则应用该快照来恢复状态
			sc.applySnapshot(applyMsg.Snapshot)
		} else if applyMsg.CommandValid { // 如果收到的是有效的命令，则尝试将命令转换为 Op 类型并执行
			op, ok := applyMsg.Command.(Op) // 尝试将 Command 断言为 Op 类型
			if !ok {
				actualType := reflect.TypeOf(applyMsg.Command)
				scLogger.Error(
					logger.LT_CtrlServer,
					"%%%d: Command is not of type *Op, actual type: %v\n", sc.me, actualType,
				)
			}
			if op.Type == OP_NOOP || sc.isExecuted(&op) {
				// 如果是 NOOP 操作或者操作已经执行过，则不做任何处理
			} else {
				sc.applyCommand(op, applyMsg.CommandIndex)
			}
		}
		sc.mu.Unlock()
	}
}

func (sc *ShardCtrler) noopTicker() {
	for !sc.killed() {
		if sc.isLeader() {
			op := Op{Type: OP_NOOP}
			sc.rf.Start(op)
		}
		time.Sleep(time.Duration(noopTickIntervalMs) * time.Millisecond)
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.configs = make([]Config, 0)
	initConfig := Config{
		Num:    0,
		Shards: [NShards]int{}, // 使用{}表示空值初始化，初始值为0(the invalid group)
		Groups: make(map[int][]string),
	}
	sc.configs = []Config{initConfig}
	sc.nextOpSeqno = make(map[int]int)
	sc.notifiers = make(map[int]Notifier)

	go sc.executor()
	go sc.noopTicker()

	return sc
}
