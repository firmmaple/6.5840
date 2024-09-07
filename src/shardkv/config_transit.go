package shardkv

import (
	"time"

	"6.5840/logger"
	"6.5840/shardctrler"
)

type requestShardArgs struct {
	Id          identification
	VersRequest int
	Shard       int
}

type requestShardReply struct {
	ShardDB     map[string]string
	NextOpSeqno map[int]int
	Err         Err
}

func getDBCopy(data map[string]string) map[string]string {
	dataCopy := make(map[string]string)
	for key, value := range data {
		dataCopy[key] = value
	}

	return dataCopy
}

func getNextSeqnoCopy(data map[int]int) map[int]int {
	dataCopy := make(map[int]int)
	for key, value := range data {
		dataCopy[key] = value
	}

	return dataCopy
}

func (kv *ShardKV) sendRequestShard(
	gid int, args *requestShardArgs, reply *requestShardReply,
) bool {
	servers := kv.curConfig.Groups[gid]
	for {
		for si := 0; si < len(servers); si++ {
			srv := kv.make_end(servers[si])
			var r requestShardReply
			ok := srv.Call("ShardKV.RequestShard", args, &r)
			if !(ok && r.Err == ErrWrongLeader) {
				*reply = r
				return ok
			}
		}
	}
}

func (kv *ShardKV) RequestShard(args *requestShardArgs, reply *requestShardReply) {
	kvLogger.Trace(
		logger.LT_SERVER, "%v handling shard request <%d, %d>\n",
		kv.id, args.VersRequest, args.Shard,
	)

	kv.mu.Lock()
	defer kv.mu.Unlock()
	if args.VersRequest+1 > kv.nextConfig.Num {
		kvLogger.Trace(
			logger.LT_SERVER, "%v is less update(%d) than %%%v(%d), intransition: %v\n",
			kv.id, kv.nextConfig.Num, args.Id, args.VersRequest+1, kv.isInTransition(),
		)
		reply.Err = WaitForUpdate
		return

		// kv.mu.Unlock()
		// latestConfig := kv.sm.Query(-1)
		// kv.mu.Lock()
		// if kv.config.Num+1 == latestConfig.Num {
		// 	op := Op{Type: OP_START_CONFIG, ClerkId: -1, NewConfig: latestConfig}
		// 	kv.rf.Start(op)
		// } else if kv.config.Num != latestConfig.Num {
		// 	// 有可能和configpoller并发执行
		// 	kvLogger.Trace(
		// 		logger.LT_SERVER, "%v: old config num: %d, latest config num %d\n",
		// 		kv.id, kv.config.Num, latestConfig.Num,
		// 	)
		// }
	}

	if kv.shardDBs[args.Shard].Vers != args.VersRequest {
		kvLogger.Debug(
			logger.LT_SERVER,
			"%v do not have vers %d of shard %d, only vers %d\n",
			kv.id, args.VersRequest, args.Shard, kv.shardDBs[args.Shard].Vers,
		)
		reply.Err = ErrWrongGroup
		return
	}

	reply.ShardDB = getDBCopy(kv.shardDBs[args.Shard].Db)
	reply.NextOpSeqno = getNextSeqnoCopy(kv.nextOpSeqno[args.Shard])
	reply.Err = OK
}

func (kv *ShardKV) installShard(shard int) {
	kv.mu.Lock()
	kvLogger.Debug(logger.LT_Shard, "%v: attempt to get shard %d\n", kv.id, shard)
	gid := kv.curConfig.Shards[shard]
	args := requestShardArgs{Id: kv.id, VersRequest: kv.curConfig.Num, Shard: shard}
	var reply requestShardReply
	kv.mu.Unlock()

	for !kv.killed() && kv.isLeader() {
		reply = requestShardReply{}
		ok := kv.sendRequestShard(gid, &args, &reply)
		if ok && reply.Err == OK {
			kvLogger.Debug(
				logger.LT_Shard, "%v got shard (%v, %v)\n",
				kv.id, args.VersRequest, shard,
			)
			break
		}
		kvLogger.Debug(
			logger.LT_Shard, "%v request shard (%v, %v) failed(Ok: %v, Err: %v), try again\n",
			kv.id, args.VersRequest, shard, ok, reply.Err,
		)
		time.Sleep(100 * time.Millisecond)
	}

	kv.mu.Lock()
	op := Op{
		Type:        OP_INSTALL_SHARD,
		ClerkId:     -1,
		Shard:       shard,
		NextOpSeqno: getNextSeqnoCopy(reply.NextOpSeqno),
		ShardVers:   args.VersRequest,
		ShardData:   getDBCopy(reply.ShardDB),
	}
	kv.mu.Unlock()

	kv.rf.Start(op)
	// for {
	// 	kv.rf.Start(op)
	// 	time.Sleep(2000 * time.Millisecond)
	// 	if kv.killed() || !kv.isLeader() {
	// 		return
	// 	}
	// 	kv.mu.Lock()
	// 	if kv.shardDBs[shard].vers == kv.config.Num {
	// 		kv.mu.Unlock()
	// 		return
	// 	}
	// }
}

func (kv *ShardKV) transitConfig(configNum int) {
	if kv.nextConfig.Num > configNum {
		return
	}
	curServingShards := kv.servingShards
	nextServingShards := make([]int, 0)
	for i := 0; i < NShards; i++ {
		if kv.nextConfig.Shards[i] == kv.gid {
			nextServingShards = append(nextServingShards, i)
		}
	}
	kv.servingShards = nextServingShards

	// todo vers不是最新的也需要trasnit
	missingShards := make([]int, 0)
	curIndex, nextIndex := 0, 0
	for nextIndex < len(nextServingShards) {
		if curIndex >= len(curServingShards) ||
			curServingShards[curIndex] > nextServingShards[nextIndex] {
			missingShards = append(missingShards, nextServingShards[nextIndex])
			nextIndex++
		} else if curServingShards[curIndex] < nextServingShards[nextIndex] {
			curIndex++
		} else {
			shard := curServingShards[curIndex]
			if kv.shardDBs[shard].Vers+1 == configNum {
				// 如果server拥有该shard的最新版本
				// 则将更新shard version为下一config num
				kv.shardDBs[shard].Vers += 1
			} else {
				// 如果server拥有该shard，但不是最新版本
				missingShards = append(missingShards, shard)
			}
			curIndex++
			nextIndex++
		}
	}
	if len(missingShards) == 0 {
		kvLogger.Debug(
			logger.LT_Shard,
			"%v: transit to %v finished(no missing shards)\n",
			kv.id, kv.nextConfig.Num,
		)
		kv.curConfig = kv.nextConfig
	} else if kv.isLeader() {
		kvLogger.Debug(logger.LT_Shard, "%v: missing shards: %v\n", kv.id, missingShards)

		for _, shard := range missingShards {
			go kv.installShard(shard)
		}
	}
}

// todo 添加config队列，一个一个transit config
func (kv *ShardKV) configPoller() {
	configQeueu := make([]shardctrler.Config, 0)
	for !kv.killed() {
		if kv.isLeader() {
			latestConfig := kv.sm.Query(-1)
			kv.mu.Lock()
			kvLogger.Debug(
				logger.LT_Shard, "%v config num %d, latest config num %d\n",
				kv.id, kv.curConfig.Num, latestConfig.Num,
			)
			// 判断是否需要将newestConfig加入configQeueu
			lastConfig := kv.nextConfig
			if len(configQeueu) != 0 {
				lastConfig = configQeueu[len(configQeueu)-1]
			}
			if lastConfig.Num+1 == latestConfig.Num {
				configQeueu = append(configQeueu, latestConfig)
				kvLogger.Debug(
					logger.LT_Shard, "%v append latest config (num %v)\n",
					kv.id, latestConfig.Num,
				)
			} else if lastConfig.Num+1 < latestConfig.Num {
				// 获取中间缺失的config
				kv.mu.Unlock()
				for num := lastConfig.Num + 1; num <= latestConfig.Num; num++ {
					nextConfig := kv.sm.Query(num)
					if lastConfig.Num+1 != nextConfig.Num {
						kvLogger.Debug(
							logger.LT_Shard,
							"%v last config num %d, fetched next config num %d\n",
							kv.id, kv.curConfig.Num, latestConfig.Num,
						)
						break
					}
					kvLogger.Debug(
						logger.LT_Shard, "%v append missing config (num %v)\n",
						kv.id, nextConfig.Num,
					)
					configQeueu = append(configQeueu, nextConfig)
					lastConfig = nextConfig
				}
				kv.mu.Lock()
			}
			// 如果上一个config已transit，transit下一个config
			if kv.isInTransition() == false && len(configQeueu) != 0 {
				nextConfig := configQeueu[0]
				configQeueu = configQeueu[1:]
				kvLogger.Debug(
					logger.LT_SERVER,
					"%v submit new config %d\n",
					kv.id, nextConfig.Num,
				)
				op := Op{Type: OP_START_CONFIG, ClerkId: -1, NewConfig: nextConfig}
				// todo 如果在这时候恰好发生了leadership change，是不是会出现问题
				kv.rf.Start(op)
			} else {
				kvLogger.Debug(
					logger.LT_Shard,
					"%v cannot transit config(inTransition: %v, len(configQeueu): %v)\n",
					kv.id, kv.isInTransition(), len(configQeueu),
				)
			}
			kv.mu.Unlock()
		}
		time.Sleep(pollIntervialMS * time.Millisecond)
	}
}
