package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/logger"
)

type electionState int

const (
	LEADER electionState = iota
	FOLLOWER
	CANDIDATE
)

const MS_HEARTBEAT_INTERVAL = 100

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type logEntry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	CurrentTerm    int // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	VotedFor       int // candidateId that received vote in current term (or -1 if none)
	hasReceivedMsg bool
	voteCount      int
	eState         electionState

	Log         []logEntry // log entries
	commitIndex int        // index of highest log entry known to be committed (initialized to 0)
	lastAppiled int        // index of highest log entry applied to state machine (initialized to 0)
	nextIndex   []int      // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex  []int      // for each server, index of highest log entry known to be replicated on server (initialized to 0)
	cond        sync.Cond
	applyCh     chan ApplyMsg

	snapshot      []byte
	snapshotIndex int
	snapshotTerm  int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.CurrentTerm
	isleader = (rf.eState == LEADER)
	return term, isleader
}

// hasTerm 从给定索引index开始向前搜索服务器日志，寻找具有指定任期term的条目。
// 如果找到，返回该日志条目的索引；如果没有找到，返回 -1。
func (rf *Raft) hasTerm(term int, index int) int {
	for i := index; i > rf.snapshotIndex; i-- {
		if rf.logAtIndex(i).Term == term {
			return i
		}
	}

	if rf.snapshotTerm == term {
		return rf.snapshotIndex
	}

	return -1
}

func (rf *Raft) logLength() int {
	return rf.snapshotIndex + 1 + len(rf.Log)
}

func (rf *Raft) logAtIndex(index int) logEntry {
	if index-rf.snapshotLength() < 0 || index-rf.snapshotLength() >= len(rf.Log) {
		logger.Warn(
			logger.LT_Log,
			"%%%d: log index out of range - index: %d, snapshot length: %d\n",
			rf.me, index, rf.snapshotLength(),
		)
	}
	return rf.Log[index-rf.snapshotLength()]
}

func (rf *Raft) logSlice(start int, end int) []logEntry {
	// logger.Trace(
	// 	logger.LT_Log,
	// 	"%d: before slice start: %d, end: %d, ss length %d\n",
	// 	rf.me, start, end, rf.snapshotLength(),
	// )
	// 正确范围应当是
	// snapshotLength <= start <= end <= logLength
	// 调整 start 和 end 确保它们位于快照之后的有效范围内
	if start < rf.snapshotLength() {
		start = rf.snapshotLength()
	}
	if end == -1 || end > rf.logLength() {
		end = rf.logLength()
	}

	// 转换 start 和 end 以对应于 Log 切片的实际索引
	start -= rf.snapshotLength()
	end -= rf.snapshotLength()
	// 防止反向切片
	if start > end {
		start = end
	}

	// logger.Trace(logger.LT_Log, "%d: after slice start: %d, end: %d\n", rf.me, start, end)
	return rf.Log[start:end]
}

func (rf *Raft) snapshotLength() int {
	return rf.snapshotIndex + 1
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	// err1 := e.Encode(rf.CurrentTerm)
	// err2 := e.Encode(rf.VotedFor)
	// err3 := e.Encode(rf.Log)
	// err4 := e.Encode(rf.snapshotIndex)
	// err5 := e.Encode(rf.snapshotTerm)
	// if err1 != nil || err2 != nil || err3 != nil {
	// 	logger.Error(
	// 		logger.LT_Persist, "%d: error in encoding - currentTerm: %v, votedFor: %v, log: %v\n",
	// 		rf.me, rf.CurrentTerm, rf.VotedFor, rf.Log,
	// 	)
	// }
	if e.Encode(rf.CurrentTerm) != nil || e.Encode(rf.VotedFor) != nil || e.Encode(rf.Log) != nil ||
		e.Encode(rf.snapshotIndex) != nil || e.Encode(rf.snapshotTerm) != nil {
		logger.Errorln(logger.LT_Persist, "Failed to encode some fields")
	}
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
	logger.Trace(
		logger.LT_Persist,
		"%%%d successfully persist state - Term %d, votedFor %d, log len %d, sIndex %d, sTerm %d\n",
		rf.me, rf.CurrentTerm, rf.VotedFor, rf.logLength(), rf.snapshotIndex, rf.snapshotTerm,
	)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		logger.Trace(logger.LT_Persist, "%%%d bootstrap without any state\n", rf.me)
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	currentTerm := -1
	votedFor := -2
	var log []logEntry
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil ||
		d.Decode(&rf.snapshotIndex) != nil || d.Decode(&rf.snapshotTerm) != nil {
		logger.Error(
			logger.LT_Persist,
			"%%%d: error in decoding - currentTerm: %d, votedFor: %d, log length: %d\n",
			rf.me, currentTerm, votedFor, rf.logLength(),
		)
	} else {
		rf.CurrentTerm = currentTerm
		rf.VotedFor = votedFor
		rf.Log = log
		rf.snapshot = rf.persister.ReadSnapshot()
		logger.Trace(
			logger.LT_Persist,
			"%%%d successfully read state - currentTerm: %d, votedFor: %d, log length: %d\n",
			rf.me, currentTerm, votedFor, rf.logLength(),
		)
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	logger.Debug(
		logger.LT_Snap,
		"%%%d attempt to create a snapshot up to index %d(last index %d) on term %d\n",
		rf.me, index, rf.logLength()-1, rf.CurrentTerm,
	)
	// 下面三行代码顺序非常重要，不能变动
	rf.snapshotTerm = rf.logAtIndex(index).Term
	rf.Log = rf.logSlice(index+1, -1)
	rf.snapshotIndex = index
	rf.snapshot = snapshot

	// logger.Trace(
	// 	logger.LT_Snap,
	// 	"%%%d: after snapshot - snapshotTerm %d, snapshotIndex %d, log length %d\n",
	// 	rf.me, rf.snapshotTerm, rf.snapshotIndex, len(rf.Log),
	// )
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int // Candidate's term
	CandidateID  int // Candidate requesting vote
	LastLogIndex int // Index of candidate's last log entry
	LastLogTerm  int // Term of candidate's last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int  // CurrentTerm, for candidate to update itself
	VoteGranted bool // True means candidate received vote
}

type AppendEntriesArgs struct {
	Term         int        // Leader's term
	LeaderId     int        // So follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []logEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader's commitIndex
}

type AppendEntriesReply struct {
	Term   int  // currentTerm, for leader to update itself
	Sucess bool // True if follower contained entry matching prevLogIndex and prevLogTerm
	XTerm  int  // Term in the conflicting entry (if any)
	XIndex int  // Index of first entry with that term (if any)
	XLen   int  // Log length
}

type InstallSnapshotArgs struct {
	Term              int    // leader's term
	LeaderId          int    // So follower can redirect clients
	LastIncludedIndex int    // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    // term of lastIncludedIndex
	Offset            int    // byte offset where chunk is positioned in the snapshot file
	Data              []byte // raw bytes of the snapshot chunk, starting at offset
	Done              bool   // true if this is the last chunk
}

type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Set default reply
	reply.VoteGranted = false
	reply.Term = rf.CurrentTerm

	if args.Term < rf.CurrentTerm {
		logger.Debug(
			logger.LT_Vote,
			"%%%d received stale RequestVote RPC from candidate %%%d(stale term: %d) on term %d\n",
			rf.me, args.CandidateID, args.Term, rf.CurrentTerm,
		)
		return
	} else if args.Term > rf.CurrentTerm {
		rf.eState = FOLLOWER
		rf.VotedFor = -1
		rf.CurrentTerm = args.Term
		logger.Debug(logger.LT_Vote, "%%%d converted to follower on term %d\n", rf.me, rf.CurrentTerm)
	}

	if rf.VotedFor != -1 && rf.VotedFor != args.CandidateID {
		logger.Debug(
			logger.LT_Vote,
			"%%%d refused to vote for candidate %%%d(it has already voted for %%%d) on term %d\n",
			rf.me, args.CandidateID, rf.VotedFor, rf.CurrentTerm,
		)
		return
	}
	// Server A is more update than B if
	// 1. A has higger term in last entry,
	// or
	// 2. they have the same term, but A's log is longer the B
	lastLogTerm := rf.snapshotTerm
	if len(rf.Log) != 0 {
		lastLogTerm = rf.logAtIndex(rf.logLength() - 1).Term
	}
	if args.LastLogTerm > lastLogTerm {
		// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
		rf.VotedFor = args.CandidateID
		reply.VoteGranted = true
		rf.hasReceivedMsg = true
		logger.Debug(
			logger.LT_Vote, "%%%d vote candidate %%%d on term %d\n",
			rf.me, args.CandidateID, rf.CurrentTerm,
		)
	} else if args.LastLogTerm == lastLogTerm && args.LastLogIndex+1 >= rf.logLength() {
		reply.VoteGranted = true
		rf.hasReceivedMsg = true
		logger.Debug(logger.LT_Vote, "%%%d vote for candidate %%%d(Repeat Vote=%v) on term %d\n",
			rf.me, args.CandidateID, rf.VotedFor == args.CandidateID, rf.CurrentTerm)
		rf.VotedFor = args.CandidateID
	} else {
		logger.Debug(logger.LT_Vote, "%%%d refused to vote for candidate %%%d since itself is more update on term %d\n",
			rf.me, args.CandidateID, rf.CurrentTerm)
	}

	rf.persist()
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist() // LIFO

	// Set default reply
	reply.Term = rf.CurrentTerm
	reply.Sucess = false

	if args.Term < rf.CurrentTerm {
		logger.Debug(
			logger.LT_Client,
			"%%%d received stale AppendEntries RPC from leader %%%d (stale term: %d) on term %d\n",
			rf.me, args.LeaderId, args.Term, rf.CurrentTerm,
		)
		return
	} else if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.eState = FOLLOWER
		logger.Debug(
			logger.LT_Client, "%%%d update its term to %d\n", rf.me, rf.CurrentTerm,
		)
		rf.VotedFor = args.LeaderId
	} else if rf.eState == CANDIDATE {
		// If AppendEntries RPC received from new leader: convert to follower
		rf.eState = FOLLOWER
		rf.VotedFor = args.LeaderId
		logger.Debug(
			logger.LT_Candidate,
			"%%%d abstained from election on term %d\n",
			rf.me, rf.CurrentTerm,
		)
	}

	rf.hasReceivedMsg = true
	rf.VotedFor = args.LeaderId

	// Rule 2: First we need to check whether follower has an entry at PreLogIndex
	if rf.logLength() <= args.PrevLogIndex {
		reply.XLen = rf.logLength()
		logger.Debug(
			logger.LT_Client,
			"%%%d only has %d entries. PrevLogIndex %d is too long\n",
			rf.me, rf.logLength(), args.PrevLogIndex,
		)
		return
	}
	// Rule 2: Check for log consistency between leader and follower
	if args.PrevLogIndex < rf.snapshotIndex {
		// 发现了stale AppendEntries RPC
		// 因为Snapshot()是client调用的，所以只有log entry被apply之后，
		// Snapshot()才会被调用，所以 snapshotIndex <= lastAppiled <= commitIndex
		// 所以snapshot中的条目肯定不会和leader的条目产生冲突
		// 什么都不做，之后检查args.Entries[]的条目是否一致就好
	} else if args.PrevLogIndex == rf.snapshotIndex {
		if rf.snapshotTerm != args.PrevLogTerm {
			// 不可能会不一样吧？
			logger.Error(
				logger.LT_Client,
				"%%%d inconsistency between snapshotTerm %d and PrevLogTerm %d on term %d\n",
				rf.me, rf.snapshotTerm, args.PrevLogTerm, rf.CurrentTerm,
			)
		}
	} else if rf.logAtIndex(args.PrevLogIndex).Term != args.PrevLogTerm {
		// 如果发生冲突，将冲突条目的term记录为XTerm，并寻找索引xIndex，
		// xIndex是日志中，第一个term为XTerm的条目
		xIndex := args.PrevLogIndex
		reply.XTerm = rf.logAtIndex(xIndex).Term
		// 因为snapshot中的元素肯定不会有冲突，所以只需要检查snapshotIndex+1到xindex-1的条目即可
		for ; xIndex-1 > rf.snapshotIndex; xIndex-- {
			if rf.logAtIndex(xIndex-1).Term != reply.XTerm {
				break
			}
		}
		reply.XIndex = xIndex
		reply.XLen = rf.logLength()
		logger.Debug(
			logger.LT_Client,
			"%%%d has entry[%d] conflicts with leader %%%d's [%d] at index %d on term %d\n",
			rf.me, rf.logAtIndex(args.PrevLogIndex).Term,
			args.LeaderId, args.PrevLogTerm, args.PrevLogIndex, rf.CurrentTerm,
		)
		return
	}

	if args.Entries == nil || len(args.Entries) == 0 {
		logger.Debug(
			logger.LT_Client,
			"%%%d received heartbeart(prev log index: %d) from leader %%%d on term %d\n",
			rf.me, args.PrevLogIndex, args.LeaderId, rf.CurrentTerm,
		)
	}

	// Append any new entries not already in the log
	offset := 0
	if args.PrevLogIndex < rf.snapshotIndex {
		offset = rf.snapshotIndex - args.PrevLogIndex
	}
	for ; offset < len(args.Entries); offset++ {
		entryIndex := args.PrevLogIndex + offset + 1
		if entryIndex >= rf.logLength() ||
			rf.logAtIndex(entryIndex).Term != args.Entries[offset].Term {
			// If an existing entry conflicts with a new one (same index but different terms),
			// delete the existing entry and all that follow it
			rf.Log = rf.logSlice(0, entryIndex)
			// Append any new entries not already in the log
			// logger.Warn(logger.LT_Log, "%d log len %d\n", rf.me, len(rf.log))
			rf.Log = append(rf.Log, args.Entries[offset:]...)
			// logger.Warn(logger.LT_Log, "%d log len %d\n", rf.me, len(rf.log))
			logger.Info(
				logger.LT_Client,
				"%%%d successfully append entry %v to itself, now has %d entries, on term %d\n",
				rf.me, args.Entries[offset:], rf.logLength(), rf.CurrentTerm,
			)
			break
		}
	}

	reply.Sucess = true
	if args.LeaderCommit > rf.commitIndex {
		originCommitIndex := rf.commitIndex
		rf.commitIndex = min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
		logger.Trace(
			logger.LT_Client,
			"%%%d follow its leader %%%d's commitIndex changing it from %d to %d on term %d\n",
			rf.me, args.LeaderId, originCommitIndex, rf.commitIndex, rf.CurrentTerm,
		)
		rf.cond.Broadcast()
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Set default reply
	reply.Term = rf.CurrentTerm

	// Rule 1: Reply immediately if term < currentTerm
	if args.Term < rf.CurrentTerm {
		logger.Debug(
			logger.LT_Snap,
			"%%%d received stale InstallSnapshot RPC(stale term: %d) on term %d\n",
			rf.me, args.Term, rf.CurrentTerm,
		)
		return
	} else if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.eState = FOLLOWER
		logger.Debug(
			logger.LT_Snap, "%%%d update its term to %d\n", rf.me, rf.CurrentTerm,
		)
		rf.VotedFor = args.LeaderId
	}

	rf.hasReceivedMsg = true
	rf.VotedFor = args.LeaderId

	if args.Offset != 0 || args.Done != true {
		logger.Error(
			logger.LT_Snap,
			"%d: chunk is not implemented yet. Entire snapshot is sent in a single InstallSnapshot RPC\n",
			rf.me,
		)
		return
	}

	if args.LastIncludedIndex <= rf.snapshotIndex {
		logger.Trace(
			logger.LT_Snap,
			"%%%d received stale InstallSnapshot RPC(snapshot index: %d < %d) on term %d\n",
			rf.me, args.LastIncludedIndex, rf.snapshotIndex, rf.CurrentTerm,
		)
		return
	} else if rf.logLength()-1 < args.LastIncludedIndex {
		originalIndex := rf.snapshotIndex
		rf.snapshotIndex = args.LastIncludedIndex
		rf.snapshotTerm = args.LastIncludedTerm
		rf.snapshot = args.Data
		rf.Log = make([]logEntry, 0)
		logger.Trace(
			logger.LT_Snap,
			"%%%d updated its snapshot from index %d to %d on term %d\n",
			rf.me, originalIndex, args.LastIncludedIndex, rf.CurrentTerm,
		)
	} else {
		// 因为 rf.snapshotIndex < args.LastIncludedIndex < rf.logLength()
		// 所以rf.logAtIndex(args.LastIncludedIndex)不会越界
		if rf.logAtIndex(args.LastIncludedIndex).Term != args.Term {
			originalLogLength := rf.logLength()
			originalSnapshotIndex := rf.snapshotIndex
			rf.snapshotIndex = args.LastIncludedIndex
			rf.snapshotTerm = args.LastIncludedTerm
			rf.snapshot = args.Data
			rf.Log = make([]logEntry, 0)
			logger.Info(
				logger.LT_Snap,
				"%%%d discarded entrie log(length %d) and created snapshot(index %d -> %d) on term %d\n",
				rf.me, originalLogLength, originalSnapshotIndex, args.LastIncludedIndex, rf.CurrentTerm,
			)
		} else {
			// Rule 6: If existing log entry has same index and term as snapshot’s
			//         last included entry, retain log entries following it and reply
			logger.Info(
				logger.LT_Snap,
				"%%%d: existing log entry has same index %d and term %d as snapshot’s last included entry\n",
				rf.me, args.LastIncludedIndex, args.LastIncludedTerm,
			)
			return
		}
	}
	rf.lastAppiled = args.LastIncludedIndex
	rf.commitIndex = args.LastIncludedIndex
	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      rf.snapshot,
		SnapshotTerm:  rf.snapshotTerm,
		SnapshotIndex: rf.snapshotIndex,
	}
	rf.mu.Unlock()
	rf.applyCh <- msg
	rf.mu.Lock()
	logger.Info(
		logger.LT_APPLIER, "%%%d applied <Snapshot Index %d, Snapshot Term %d> on term %d\n",
		rf.me, msg.SnapshotIndex, msg.SnapshotTerm, rf.CurrentTerm,
	)
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	return ok
}

func (rf *Raft) sendAppendEntries(
	server int, args *AppendEntriesArgs, reply *AppendEntriesReply,
) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	return ok
}

func (rf *Raft) sendInstallSnapshot(
	server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply,
) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)

	return ok
}

func (rf *Raft) sendHeartBeat(client int) {
	rf.mu.Lock()
	if rf.killed() || rf.eState != LEADER {
		rf.mu.Unlock()
		return
	}
	sentTerm := rf.CurrentTerm
	logger.Debug(
		logger.LT_Leader, "%%%d attempt to send heartbeat to %%%d on term %d\n",
		rf.me, client, sentTerm,
	)
	// TODO 稍微有点问题，如果follower的日志有残缺，那么snapshotIndex会作为prevLogIndex的下限
	// 但是随之而来的问题是，folloer在处理appendentries是，会发生冲突然后直接返回，
	// 不会打印接收到心跳的日志信息，到时候得改一改
	prevLogIndex := rf.snapshotIndex
	prevLogTerm := rf.snapshotTerm
	if rf.nextIndex[client]-1 > rf.snapshotIndex {
		prevLogIndex = rf.nextIndex[client] - 1
		prevLogTerm = rf.logAtIndex(prevLogIndex).Term
	}
	args := AppendEntriesArgs{
		Term:         sentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()
	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(client, &args, &reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ok {
		if sentTerm < rf.CurrentTerm {
			logger.Trace(
				logger.LT_Leader,
				"%%%d heartbeat to %%%d(sent on term %d) failed on term %d\n",
				rf.me, client, sentTerm, rf.CurrentTerm,
			)
		} else {
			logger.Debug(
				logger.LT_Leader,
				"%%%d cannot sent heartbeat to %d on term %d\n",
				rf.me, client, rf.CurrentTerm,
			)
		}
		return
	}
	logger.Info(
		logger.LT_Leader,
		"%%%d sent heartbeat to server %%%d on term %d, received reply on term %d\n",
		rf.me, client, sentTerm, rf.CurrentTerm,
	)
	if reply.Term > rf.CurrentTerm {
		rf.eState = FOLLOWER
		rf.CurrentTerm = reply.Term
		logger.Debug(
			logger.LT_Client, "%%%d update its term to %d and convert to follower\n",
			rf.me, rf.CurrentTerm,
		)
		rf.persist()
	}
}

// Send heartbeat to all server periodically
func (rf *Raft) heartBeatSender() {
	for {
		rf.mu.Lock()
		if rf.killed() || rf.eState != LEADER {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		for i := range rf.peers { // send heartbeat to each server
			if i != rf.me {
				go rf.sendHeartBeat(i)
			}
		}
		time.Sleep(MS_HEARTBEAT_INTERVAL * time.Millisecond)
	}
}

// 要传一下term 和 len(rf.log) ?
func (rf *Raft) replicate(
	client int, replicateTerm int, lastLogIndex int, finishedClient chan int,
) {
	rf.mu.Lock()
	if rf.nextIndex[client] > lastLogIndex {
		logger.Warn(
			logger.LT_Leader,
			"%%%d's lastLogIndex %d < client %%%d's nextIndex %d when replicate\n",
			rf.me, lastLogIndex, client, rf.nextIndex[client],
		)
		rf.mu.Unlock()
		return
	}

	for {
		var prevLogIndex int
		var prevLogTerm int
		if rf.nextIndex[client]-1 > rf.snapshotIndex {
			prevLogIndex = rf.nextIndex[client] - 1
			prevLogTerm = rf.logAtIndex(prevLogIndex).Term
		} else if rf.nextIndex[client]-1 == rf.snapshotIndex {
			prevLogIndex = rf.snapshotIndex
			prevLogTerm = rf.snapshotTerm
		} else {
			// TODO 发送installsnapshot
			rf.mu.Unlock()
			if rf.fillGap(client, replicateTerm) == false {
				logger.Trace(
					logger.LT_Leader, "%%%d stop replicating to %%%d since InstallSnapshot failute\n",
					rf.me, client,
				)
				return
			}
			rf.mu.Lock()
			prevLogIndex = rf.snapshotIndex
			prevLogTerm = rf.snapshotTerm
		}

		// 如果不采用深拷贝，可能会产生data race
		// RPC调用时读取args的Entries没有锁，这时候如果其他goroutine在写的话，会产生data race
		entries := make([]logEntry, rf.logLength()-prevLogIndex-1)
		copy(entries, rf.logSlice(prevLogIndex+1, -1))
		args := AppendEntriesArgs{
			Term:         replicateTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
		}
		reply := AppendEntriesReply{}
		logger.Trace(
			logger.LT_Leader, "%%%d attempt to replicate entries %v to %%%d on term %d\n",
			rf.me, args.Entries, client, replicateTerm,
		)
		rf.mu.Unlock()

		ok := rf.sendAppendEntries(client, &args, &reply)
		rf.mu.Lock()
		if rf.killed() || rf.eState != LEADER {
			logger.Trace(
				logger.LT_Leader,
				"%%%d stop sendAppendEntries(killed: %v, leader: %v) on term %d\n",
				rf.me, rf.killed(), rf.eState == LEADER, rf.CurrentTerm,
			)
			rf.mu.Unlock()
			return
		} else if ok {
			if reply.Sucess {
				// rf.nextIndex[client] = len(rf.log)
				// rf.matchIndex[client] = len(rf.log) - 1
				// 假设目前 log = [nil, 1], matchIndex = 1, nextIndex = 2
				// 然后client请求执行cmd 2，于是leader将2加入log，log = [nil, 1, 2]
				// 然后leader发出replicate A，replicate [2]
				// 但是在A得到回复前，client请求执行cmd 3，于是leader将3加入log，log = [nil, 1, 2, 3]
				// 并向client发出replicate B， replicate [2, 3]
				// 如果B先于A执行，执行后 matchIndex = 3， nextIndex = 4
				// 之后再执行A，这时候我们保持matchIndex和nextIndex不变就好
				rf.matchIndex[client] = max(rf.matchIndex[client], prevLogIndex+len(args.Entries))
				rf.nextIndex[client] = rf.matchIndex[client] + 1
				logger.Info(
					logger.LT_Leader,
					"%%%d succeeded in replicating entry %v to %%%d(sent on term %d) on term %d\n",
					rf.me, args.Entries, client, replicateTerm, rf.CurrentTerm,
				)
				break
			} else {
				if rf.CurrentTerm < reply.Term {
					rf.CurrentTerm = reply.Term
					rf.eState = FOLLOWER
					logger.Debug(
						logger.LT_Leader,
						"%%%d update its term to %d and convert to follower\n",
						rf.me, rf.CurrentTerm,
					)
					rf.persist()
					rf.mu.Unlock()
					return
				} else if rf.CurrentTerm > replicateTerm { // Simply drop the RPC response from the old term
					logger.Trace(
						logger.LT_Leader,
						"%%%d drop an old appendEntries response(sent on term %d) on current term %d\n",
						rf.me, replicateTerm, rf.CurrentTerm,
					)
					rf.mu.Unlock()
					return
				}
				if reply.XLen <= args.PrevLogIndex {
					// Case 3: follower's log is too short:
					//   nextIndex = XLen
					rf.nextIndex[client] = reply.XLen
					logger.Trace(
						logger.LT_Leader, "%d fast backup case 3 - follower's log is too short, only %d\n",
						rf.me, reply.XLen,
					)
				} else if lastEntryIndex := rf.hasTerm(reply.XTerm, args.PrevLogIndex); lastEntryIndex == -1 {
					// Case 1: leader doesn't have XTerm:
					//   nextIndex = XIndex
					logger.Trace(
						logger.LT_Leader,
						"%d fast backup case 1 - leader doesn't have the term %d and next index will be %d\n",
						rf.me, reply.XTerm, reply.XIndex,
					)
					rf.nextIndex[client] = reply.XIndex
				} else {
					// Case 2: leader has XTerm:
					//   nextIndex = leader's last entry for XTerm
					logger.Trace(
						logger.LT_Leader, "%d fast backup case 2 - leadder has XTerm %d and its last index is %d\n",
						rf.me, reply.XTerm, lastEntryIndex,
					)
					rf.nextIndex[client] = lastEntryIndex
				}
				logger.Debug(
					logger.LT_Leader,
					"%%%d: AppendEntries to %%%d failed(sent on term %d) beacause of inconsistency on term %d, retry again\n",
					rf.me, client, replicateTerm, rf.CurrentTerm,
				)
			}
		} else { // RPC failed
			logger.Trace(
				logger.LT_Leader,
				"%%%d: AppendEntries(sent on term %d) to %%%d failed, try again\n",
				rf.me, replicateTerm, client,
			)
		}
	}
	rf.mu.Unlock()

	// rf.cond.Broadcast()
	finishedClient <- client
}

func (rf *Raft) fillGap(client int, replicateTerm int) bool {
	rf.mu.Lock()
	args := InstallSnapshotArgs{
		Term:              replicateTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.snapshotIndex,
		LastIncludedTerm:  rf.snapshotTerm,
		Offset:            0,
		Data:              rf.snapshot,
		Done:              true,
	}
	reply := InstallSnapshotReply{}
	rf.mu.Unlock()

	for {
		rf.mu.Lock()
		if rf.killed() || rf.eState != LEADER {
			logger.Trace(
				logger.LT_Snap,
				"%%%d stop sendInstallSnapshot(killed: %v, leader: %v) on term %d\n",
				rf.me, rf.killed(), rf.eState == LEADER, rf.CurrentTerm,
			)
			rf.mu.Unlock()
			return false
		}
		logger.Trace(
			logger.LT_Leader,
			"%%%d attempt to InstallSnapshot(Index %d, Term %d) to %%%d on term %d\n",
			rf.me, args.LastIncludedIndex, args.LastIncludedTerm, client, rf.CurrentTerm,
		)
		rf.mu.Unlock()

		if rf.sendInstallSnapshot(client, &args, &reply) == true {
			rf.mu.Lock()
			if rf.CurrentTerm < reply.Term {
				rf.CurrentTerm = reply.Term
				rf.eState = FOLLOWER
				logger.Trace(
					logger.LT_Snap,
					"%%%d update its term to %d and convert to follower\n",
					rf.me, rf.CurrentTerm,
				)
				rf.persist()
				rf.mu.Unlock()
				return false
			} else if rf.CurrentTerm > replicateTerm { // Simply drop the RPC response from the old term
				logger.Trace(
					logger.LT_Leader,
					"%%%d drop an stale sendInstallSnapshot response(sent on term %d) on current term %d\n",
					rf.me, replicateTerm, rf.CurrentTerm,
				)
				rf.mu.Unlock()
				return false
			} else {
				logger.Info(
					logger.LT_Snap,
					"%%%d succeeded in InstallSnapshot(Index %d, Term %d) to %%%d on term %d\n",
					rf.me, args.LastIncludedIndex, args.LastIncludedTerm, client, rf.CurrentTerm,
				)
				rf.mu.Unlock()
				return true
			}
		}
	}
}

// return false if there is no need to start an election
// else start an election and return true
func (rf *Raft) startElection() bool {
	rf.mu.Lock()
	// Q：为什么要把检查条件放在startElection函数，而不是ticker函数里呢？
	// 似乎放在ticker函数里检查更符合直觉？
	// A：我最初就是这么做的，但是这样会出现一个bug，
	// 假设此刻server N 符合选举条件，但是尚未进入startElection函数执行选举代码
	// 这时另一个server M开始选举，而N将票投给了M
	// 这时M才开始执行startElection的代码，然后bug就发生了，
	// 在这一term中，M既作为follower投了票，又充当candidate参与选举
	if rf.hasReceivedMsg || rf.eState == LEADER {
		rf.mu.Unlock()
		return false
	}

	rf.voteCount = 0
	rf.CurrentTerm++
	rf.eState = CANDIDATE
	electionTerm := rf.CurrentTerm

	lastLogIndex := rf.snapshotIndex
	lastLogTerm := rf.snapshotTerm
	if len(rf.Log) != 0 {
		lastLogIndex = rf.logLength() - 1
		lastLogTerm = rf.logAtIndex(lastLogIndex).Term
	}
	logger.Info(
		logger.LT_Vote, "%%%d starts an election on term %d\n", rf.me, electionTerm,
	)
	rf.voteCount++ // Vote for itself
	rf.VotedFor = rf.me
	rf.persist()
	rf.mu.Unlock()

	isElectionFinished := false
	for i := range rf.peers { // Send RequestVoe RPCs to all other servers
		if i == rf.me {
			continue // You have alrealy vote for yourself
		}
		go func(n int) {
			args := RequestVoteArgs{
				Term:         electionTerm,
				CandidateID:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply := RequestVoteReply{}
			logger.Trace(
				logger.LT_Vote, "%%%d request %%%d to vote on term %d\n",
				rf.me, n, electionTerm,
			)
			// rf.sendRequestVote(n, &args, &reply)
			for {
				ok := rf.sendRequestVote(n, &args, &reply)
				if rf.killed() {
					logger.Trace(
						logger.LT_Vote, "%%%d stop request vote since it has been killed\n", rf.me,
					)
					return
				} else if ok {
					break
				} else {
					rf.mu.Lock()
					if electionTerm != rf.CurrentTerm {
						logger.Trace(logger.LT_Vote, "%%%d cancel an old vote requst(sent on term %d) to %%%d\n", rf.me, electionTerm, n)
					}
					rf.mu.Unlock()
					return
				}
				// logger.Trace(
				// 	logger.LT_Vote,
				// 	"%%%d: vote request(sent on term %d) to %%%d failed, try again\n",
				// 	rf.me, electionTerm, n,
				// )
			}

			// logger.Trace(logger.LT_Vote, "%%%d attempt to get lock\n", rf.me)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			logger.Trace(
				logger.LT_Vote, "%%%d get reply from %%%d(vote for me: %v) on term %d\n",
				rf.me, n, reply.VoteGranted, rf.CurrentTerm,
			)
			if !reply.VoteGranted { // Get a rejection
				if reply.Term > rf.CurrentTerm {
					rf.CurrentTerm = reply.Term
					rf.eState = FOLLOWER
					rf.persist()
				}
				return
			}
			rf.voteCount++
			if isElectionFinished {
				return
			}
			if rf.voteCount > len(rf.peers)/2 {
				isElectionFinished = true // Election finished after win the majority votes
				if electionTerm != rf.CurrentTerm {
					logger.Warn(
						logger.LT_Vote, "%%%d: a belated win(start at term %d, now on term %d)\n",
						rf.me, electionTerm, rf.CurrentTerm,
					)
					return
				}
				rf.eState = LEADER
				for j := range rf.peers {
					rf.nextIndex[j] = rf.logLength()
					rf.matchIndex[j] = 0
				}
				logger.Info(
					logger.LT_Vote, "%%%d won election on term %d\n", rf.me, rf.CurrentTerm,
				)

				go rf.heartBeatSender()
			}
		}(i)
	}

	return true
}

func (rf *Raft) commit(replicateTerm int, lastLogIndex int) {
	// Replicate
	finishedClient := make(chan int)
	for i := range rf.peers {
		if i != rf.me {
			go rf.replicate(i, replicateTerm, lastLogIndex, finishedClient)
		}
	}

	replicateCount := 1
	majority := len(rf.peers)/2 + 1
	for replicateCount < majority {
		client := <-finishedClient
		replicateCount++
		logger.Trace(
			logger.LT_Commit,
			"%%%d was notified a replicate to %%%d finished(total finished count %d)\n",
			rf.me, client, replicateCount,
		)
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	logger.Trace(
		logger.LT_Commit, "%%%d get enough replication count %d on term %d\n",
		rf.me, replicateCount, rf.CurrentTerm,
	)
	// commitIndex+1  commitIndex+2 ... len(rf.log)-1
	// 0              1             ... len(count)-1
	var i int
	countLength := rf.logLength() - 1 - rf.commitIndex
	if countLength == 0 {
		logger.Trace(
			logger.LT_Commit, "%%%d: stale command committed on term %d\n",
			rf.me, rf.CurrentTerm,
		)
		return
	}
	count := make([]int, countLength)
	for j := 0; j < len(count); j++ {
		count[j] = 1
	}
	for i = range rf.peers {
		if i != rf.me {
			index := rf.matchIndex[i] - rf.commitIndex - 1
			if index >= 0 {
				count[index]++
			}
		}
	}
	logger.Trace(logger.LT_Commit, "%%%d replic count: %v\n", rf.me, count)
	for i = countLength - 1; i >= 0; i-- {
		if count[i] >= majority && rf.logAtIndex(rf.commitIndex+1+i).Term == rf.CurrentTerm {
			newCommitIndex := rf.commitIndex + 1 + i
			logger.Debug(
				logger.LT_Leader, "%%%d update commitIndex(from %d to %d) on term %d\n",
				rf.me, rf.commitIndex, newCommitIndex, rf.CurrentTerm,
			)
			rf.commitIndex = newCommitIndex
			rf.cond.Broadcast()
			break
		}
	}
}

func (rf *Raft) applier() {
	rf.mu.Lock()
	for {
		if rf.killed() {
			break
		} else if rf.lastAppiled < rf.commitIndex {
			for {
				if rf.lastAppiled >= rf.logLength() {
					logger.Error(
						logger.LT_APPLIER, "%%%d: index(lastAppiled %d) out of range(log length %d)\n",
						rf.me, rf.lastAppiled, rf.logLength(),
					)
				}
				rf.lastAppiled++
				msg := ApplyMsg{
					CommandValid: true,
					Command:      rf.logAtIndex(rf.lastAppiled).Command,
					CommandIndex: rf.lastAppiled,
				}
				rf.mu.Unlock()
				rf.applyCh <- msg
				rf.mu.Lock()
				logger.Info(
					logger.LT_APPLIER, "%%%d applied <Command: %d, Command Index %d> on term %d\n",
					rf.me, msg.Command, msg.CommandIndex, rf.CurrentTerm,
				)
				if rf.lastAppiled == rf.commitIndex {
					break
				}
			}
		} else if rf.lastAppiled > rf.commitIndex {
			logger.Error(
				logger.LT_APPLIER, "%%%d: lastAppiled %d > commitIndex %d\n",
				rf.me, rf.lastAppiled, rf.commitIndex,
			)
		}
		rf.cond.Wait()
	}
	rf.mu.Unlock()
}

// the service using Raft () wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).
	if !rf.killed() {
		term, isLeader = rf.GetState()
		if isLeader {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			logger.Info(
				logger.LT_Commit, "%%%d received command %v on term %d\n",
				rf.me, command, rf.CurrentTerm,
			)
			// Append entry to local log
			rf.Log = append(rf.Log, logEntry{Term: rf.CurrentTerm, Command: command})
			rf.persist()
			replicateTerm := rf.CurrentTerm
			lastLogIndex := rf.logLength() - 1
			index = lastLogIndex
			go rf.commit(replicateTerm, lastLogIndex)
		}
	}

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	// unsafe
	rf.cond.Broadcast()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.
		// logger.Trace(logger.LT_Timer, "%%%d attempt tick\n", rf.me)
		rf.mu.Lock()
		logger.Trace(logger.LT_Timer, "%%%d tick on term %d\n", rf.me, rf.CurrentTerm)
		rf.mu.Unlock()

		if !rf.startElection() { // 检查是否有必要选举
			rf.mu.Lock()
			rf.hasReceivedMsg = false
			rf.mu.Unlock()
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 2*MS_HEARTBEAT_INTERVAL + 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg,
) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.hasReceivedMsg = false
	rf.voteCount = 0
	rf.eState = FOLLOWER

	// Q: Why is the Raft log 1-indexed?
	// A: You should view it as zero-indexed, but starting out with an entry
	// (at index=0) that has term 0. That allows the very first AppendEntries
	// RPC to contain 0 as PrevLogIndex, and be a valid index into the log.
	rf.Log = make([]logEntry, 0)
	rf.Log = append(rf.Log, logEntry{Term: 0})

	rf.cond = *sync.NewCond(&rf.mu)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	if rf.snapshot == nil || len(rf.snapshot) == 0 {
		rf.snapshot = nil
		rf.snapshotIndex = -1
		rf.snapshotTerm = -1
	}

	logger.Info(logger.LT_Log, "%%%d had been made\n", rf.me)

	// start ticker goroutine to start elections
	go rf.ticker()

	rf.commitIndex = max(0, rf.snapshotIndex)
	rf.lastAppiled = max(0, rf.snapshotIndex)
	go rf.applier()

	return rf
}
