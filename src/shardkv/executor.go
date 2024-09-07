package shardkv

import (
	"bytes"
	"reflect"

	"6.5840/labgob"
	"6.5840/logger"
)

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
			if !kv.isExecuted(&op) {
				kv.applyCommand(&op, applyMsg.CommandIndex)
			}
		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) applyCommand(op *Op, commandIndex int) {
	if kv.isClientComamnd(op) {
		kv.applyClientCommand(op, commandIndex)
	} else {
		kv.applyServerCommand(op)
	}
	// 检查日志大小，并在达到阈值时创建快照，以防止日志无限增长
	if kv.maxraftstate != -1 && // 如果 maxraftstate 为 -1，表示不需要创建快照
		float32(kv.persister.RaftStateSize()) > float32(kv.maxraftstate)*snapshotRatio {
		kv.createSnapshot(commandIndex) // 创建快照以压缩日志大小
	}
}

func (kv *ShardKV) applyServerCommand(op *Op) {
	kvLogger.Debug(
		logger.LT_SERVER, "%v: curConfig num: %d, op config num: %d\n",
		kv.id, kv.curConfig.Num, op.NewConfig.Num,
	)
	switch op.Type {
	case OP_START_CONFIG:
		if kv.curConfig.Num+1 > op.NewConfig.Num {
			return
		} else if kv.curConfig.Num+1 < op.NewConfig.Num {
			kvLogger.Error(
				logger.LT_SERVER, "%v: expected cur config num: %d, but get %d\n",
				kv.id, kv.curConfig.Num+1, op.NewConfig.Num,
			)
		}
		kv.nextConfig = op.NewConfig
		kv.transitConfig(kv.nextConfig.Num)
	case OP_INSTALL_SHARD:
		if kv.nextConfig.Num > op.ShardVers+1 {
			return
		} else if kv.nextConfig.Num < op.ShardVers+1 {
			kvLogger.Error(
				logger.LT_SERVER, "%v: expected next config num: %d, but get %d\n",
				kv.id, kv.nextConfig.Num, op.ShardVers+1,
			)
		} else if kv.shardDBs[op.Shard].Vers == kv.nextConfig.Num {
			kvLogger.Info(logger.LT_Test, "%v already have shard %d\n", kv.id, op.Shard)
			// 已经获得这个shard了
			return
		}
		// 将shard加入data
		kv.shardDBs[op.Shard].Db = getDBCopy(op.ShardData)
		kv.shardDBs[op.Shard].Vers = op.ShardVers + 1
		kv.nextOpSeqno[op.Shard] = getNextSeqnoCopy(op.NextOpSeqno)
		kvLogger.Info(logger.LT_Shard, "%v now serving shard %d\n", kv.id, op.Shard)
		if kv.isTransitionFinished() {
			kvLogger.Info(
				logger.LT_Shard,
				"%v: transit to %v finished(got all shards)\n",
				kv.id, kv.nextConfig.Num,
			)
			kv.curConfig = kv.nextConfig
		}
	case OP_NOOP:
	default:
		kvLogger.Error(logger.LT_SERVER, "%v: Unknow Op Type %d\n", kv.id, op.Type)
	}
	kvLogger.Debug(logger.LT_SERVER, "%v executed %v\n", kv.id, op)
}

func (kv *ShardKV) applyClientCommand(op *Op, commandIndex int) {
	switch op.Type {
	case OP_GET: // GET 操作不修改数据，因此无需处理
	case OP_PUT:
		kv.shardDBs[key2shard(op.Key)].Db[op.Key] = op.Value
	case OP_APPEND:
		originalValue, ok := kv.shardDBs[key2shard(op.Key)].Db[op.Key]
		if !ok {
			originalValue = ""
			logger.Info(logger.LT_Test, "does not have original value\n")
		}
		kv.shardDBs[key2shard(op.Key)].Db[op.Key] = originalValue + op.Value
	default:
		kvLogger.Error(logger.LT_SERVER, "%v: Unknow Op Type %d\n", kv.id, op.Type)
	}
	kvLogger.Debug(logger.LT_SERVER, "%v executed %v\n", kv.id, op)

	// 检查操作的序列号是否与预期一致，以防止操作顺序出错
	if kv.nextOpSeqno[op.Shard][op.ClerkId] != op.Seqno {
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

// createSnapshot建一个快照，该快照包含直至commandIndex的所有状态变更
func (kv *ShardKV) createSnapshot(commandIndex int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(kv.nextOpSeqno) != nil || e.Encode(kv.shardDBs) != nil ||
		e.Encode(kv.curConfig) != nil || e.Encode(kv.nextConfig) != nil {
		kvLogger.Errorln(logger.LT_Persist, "Failed to encode some fields")
	}

	kv.rf.Snapshot(commandIndex, w.Bytes())
	kvLogger.Trace(
		logger.LT_Persist, "%v made a snapshot up to commandIndex %d\n",
		kv.id, commandIndex,
	)
}

func (kv *ShardKV) applySnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		kvLogger.Trace(logger.LT_Persist, "%v bootstrap without snapshot\n", kv.id)
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	if d.Decode(&kv.nextOpSeqno) != nil || d.Decode(&kv.shardDBs) != nil ||
		d.Decode(&kv.curConfig) != nil || d.Decode(&kv.nextConfig) != nil {
		kvLogger.Errorln(logger.LT_Persist, "Failed to decode some fields")
	}

	kvLogger.Trace(
		logger.LT_Persist,
		"%v recovered from snapshot(config num: %d)\n",
		kv.id, kv.curConfig.Num,
	)
}
