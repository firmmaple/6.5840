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
	"fmt"
	"math/rand"
	"os"
	"strings"
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

// var raftLog = logger.NewLogger(logger.LL_WARN, os.Stdout, "RAFT")

// var raftLog = logger.NewLogger(logger.LL_INFO, os.Stdout, "RAFT")
// var raftLog = logger.NewLogger(logger.LL_DEBUG, os.Stdout, "RAFT")
var raftLog = logger.NewLogger(logger.LL_TRACE, os.Stdout, "RAFT")

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

type Log []logEntry

func (entry logEntry) String() string {
	return fmt.Sprintf("Term: %d - %v", entry.Term, entry.Command)
}

func (log Log) String() string {
	logLen := len(log)
	if logLen > 5 {
		return fmt.Sprintf(
			"<Total Entries Num %d - %v   %v   .....   %v   %v>",
			logLen, log[0], log[1], log[logLen-2], log[logLen-1],
		)
	} else if logLen == 1 {
		return fmt.Sprintf("%v", log[0])
	} else {
		entries := make([]string, logLen)
		for i, entry := range log {
			entries[i] = fmt.Sprintf("%v", entry)
		}
		return fmt.Sprintf("<Entries Num %d - %v>", logLen, strings.Join(entries, "   "))
	}
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
	currentTerm    int           // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor       int           // candidateId that received vote in current term (or -1 if none)
	hasReceivedMsg bool          // 在两次tick之间，server是否有收到来自leader的请求
	voteCount      int           // 在选举时，Candiadate收到的选票数目
	eState         electionState // 用于表示server的状态，是Leader、Candidate还是Follower

	log         Log   // log entries
	commitIndex int   // index of highest log entry known to be committed (initialized to 0)
	lastAppiled int   // index of highest log entry applied to state machine (initialized to 0)
	nextIndex   []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex  []int // for each server, index of highest log entry known to be replicated on server (initialized to 0)
	cond        sync.Cond
	applyCh     chan ApplyMsg

	snapshot      []byte
	snapshotIndex int // the last included index
	snapshotTerm  int // the last included term
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
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

// logLength 返回实际日志长度，包括已被快照压缩的日志部分
func (rf *Raft) logLength() int {
	return rf.snapshotIndex + 1 + len(rf.log)
}

// logAtIndex 返回给定索引index处的日志条目
// 调用者必须确保index大于快照中包含的最后一个条目的索引，即 index > snapshotIndex
func (rf *Raft) logAtIndex(index int) logEntry {
	if index-rf.snapshotLength() < 0 || index-rf.snapshotLength() >= len(rf.log) {
		raftLog.Warn(
			logger.LT_Log,
			"%%%d: log index out of range - index: %d, snapshot length: %d\n",
			rf.me, index, rf.snapshotLength(),
		)
		// 不做错误处理，因为程序运行到这里说明调用者的逻辑存在问题
	}
	return rf.log[index-rf.snapshotLength()]
}

// logSlice 返回从索引 start 到 end 的 Log 切片（不包括索引 end），即[start, end)
func (rf *Raft) logSlice(start int, end int) Log {
	// 参数start和end需要符合如下条件：
	// snapshotLength <= start <= end <= logLength
	// 如不符合，则调整 start 和 end， 确保它们位于的有效范围内
	if start < rf.snapshotLength() {
		start = rf.snapshotLength()
	}
	if end == -1 || end > rf.logLength() {
		end = rf.logLength()
	}
	// 将start 和 end 转换为 Log 切片的实际索引
	start -= rf.snapshotLength()
	end -= rf.snapshotLength()
	// 防止反向切片
	if start > end {
		start = end
	}

	return rf.log[start:end]
}

func (rf *Raft) lastLogEntry() logEntry {
	if len(rf.log) == 0 {
		return logEntry{Term: rf.snapshotTerm}
	}
	return rf.log[len(rf.log)-1]
}

// snapshotLength 返回快照中包含的日志条目总数（或者说快照压缩了多少长度的日志）
// 这等于快照中最后一个包含的日志条目的索引 + 1。
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

	if e.Encode(rf.currentTerm) != nil {
		raftLog.Error(logger.LT_Persist, "%%%d: Encode CurrentTerm: %d", rf.me, rf.currentTerm)
	}
	if e.Encode(rf.votedFor) != nil {
		raftLog.Error(logger.LT_Persist, "%%%d: Failed to encode VotedFor: %d", rf.me, rf.votedFor)
	}
	if e.Encode(rf.log) != nil {
		raftLog.Error(logger.LT_Persist, "%%%d: Failed to encode Log", rf.me)
	}
	if e.Encode(rf.snapshotIndex) != nil {
		raftLog.Error(logger.LT_Persist, "%%%d: Encode snapshotIndex: %d", rf.me, rf.snapshotIndex)
	}
	if e.Encode(rf.snapshotTerm) != nil {
		raftLog.Error(logger.LT_Persist, "%%%d: Encode snapshotTerm: %d", rf.me, rf.snapshotTerm)
	}

	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
	raftLog.Trace(
		logger.LT_Persist,
		"%%%d persisted Raft state: Term %d, VotedFor %d, LogLen %d, Snapshot(Index %d, Term %d)\n",
		rf.me, rf.currentTerm, rf.votedFor, rf.logLength(), rf.snapshotIndex, rf.snapshotTerm,
	)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		rf.snapshotIndex = -1
		rf.snapshotTerm = -1
		raftLog.Trace(logger.LT_Persist, "%%%d bootstrap without any state\n", rf.me)
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	if err := d.Decode(&rf.currentTerm); err != nil {
		raftLog.Error(logger.LT_Persist, "%%%d: error decoding currentTerm: %v\n", rf.me, err)
	}
	if err := d.Decode(&rf.votedFor); err != nil {
		raftLog.Error(logger.LT_Persist, "%%%d: error decoding votedFor: %v\n", rf.me, err)
	}
	if err := d.Decode(&rf.log); err != nil {
		raftLog.Error(logger.LT_Persist, "%%%d: error decoding log: %v\n", rf.me, err)
	}
	if err := d.Decode(&rf.snapshotIndex); err != nil {
		raftLog.Error(logger.LT_Persist, "%%%d: error decoding snapshotIndex: %v\n", rf.me, err)
	}
	if err := d.Decode(&rf.snapshotTerm); err != nil {
		raftLog.Error(logger.LT_Persist, "%%%d: error decoding snapshotTerm: %v\n", rf.me, err)
	}

	rf.snapshot = rf.persister.ReadSnapshot()
	raftLog.Trace(
		logger.LT_Persist,
		"%%%d read Raft state: Term %d, VotedFor %d, LogLen %d, Snapshot(Index %d, Term %d)\n",
		rf.me, rf.currentTerm, rf.votedFor, rf.logLength(), rf.snapshotIndex, rf.snapshotTerm,
	)
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
	if index < rf.snapshotIndex {
		raftLog.Trace(
			logger.LT_Snap,
			"%%%d already had a snapshot index(%d) more update than %d\n",
			rf.me, rf.snapshotIndex, index,
		)
    return
	}
	raftLog.Debug(
		logger.LT_Snap,
		"%%%d attempt to create a snapshot up to index %d(last log index %d) on term %d\n",
		rf.me, index, rf.logLength()-1, rf.currentTerm,
	)
	// 下面三行代码顺序非常重要，不能变动
	rf.snapshotTerm = rf.logAtIndex(index).Term
	rf.log = rf.logSlice(index+1, -1)
	rf.snapshotIndex = index
	rf.snapshot = snapshot
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
	Term         int // Leader's term
	LeaderId     int // So follower can redirect clients
	PrevLogIndex int // index of log entry immediately preceding new ones
	PrevLogTerm  int // term of prevLogIndex entry
	Entries      Log // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int // leader's commitIndex
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
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		raftLog.Debug(
			logger.LT_Vote,
			"%%%d received stale RequestVote RPC from candidate %%%d(stale term: %d) on term %d\n",
			rf.me, args.CandidateID, args.Term, rf.currentTerm,
		)
		return
	} else if args.Term > rf.currentTerm {
		rf.eState = FOLLOWER
		rf.votedFor = -1
		rf.currentTerm = args.Term
		raftLog.Debug(logger.LT_Vote, "%%%d converted to follower on term %d\n", rf.me, rf.currentTerm)
	}

	// 判断server是否在当前term，已将把选票投给其他candidate
	if rf.votedFor != -1 && rf.votedFor != args.CandidateID {
		raftLog.Debug(
			logger.LT_Vote,
			"%%%d refused to vote for candidate %%%d(it has already voted for %%%d) on term %d\n",
			rf.me, args.CandidateID, rf.votedFor, rf.currentTerm,
		)
		return
	}

	// 检查是否应该给Candidate投票。
	// Server只有在Candidate的日志至少与自己的日志一样新时才能投票给Candidate
	// Server A is more update than B if
	// 1. A has higger term in last entry,
	// or
	// 2. they have the same term, but A's log is longer the B
	lastLogTerm := rf.snapshotTerm
	if len(rf.log) != 0 {
		lastLogTerm = rf.logAtIndex(rf.logLength() - 1).Term
	}
	if args.LastLogTerm > lastLogTerm { // 候选人的日志任期号较高，授予投票
		rf.votedFor = args.CandidateID
		reply.VoteGranted = true
		rf.hasReceivedMsg = true
		raftLog.Debug(
			logger.LT_Vote, "%%%d votes for candidate %%%d on term %d\n",
			rf.me, args.CandidateID, rf.currentTerm,
		)
	} else if args.LastLogTerm == lastLogTerm && args.LastLogIndex+1 >= rf.logLength() {
		// 候选人的日志任期号相同且日志长度不短于本服务器，授予投票
		reply.VoteGranted = true
		rf.hasReceivedMsg = true
		raftLog.Debug(
			logger.LT_Vote, "%%%d votes for candidate %%%d(has voted in currently term: %v) on term %d\n",
			rf.me, args.CandidateID, rf.votedFor == args.CandidateID, rf.currentTerm,
		)
		rf.votedFor = args.CandidateID
	} else {
		// 候选人的日志不如本服务器的日志新，拒绝投票
		raftLog.Debug(
			logger.LT_Vote, "%%%d refused to vote for candidate %%%d since it is more update-to-date on term %d\n",
			rf.me, args.CandidateID, rf.currentTerm,
		)
	}

	rf.persist()
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist() // LIFO

	// Set default reply
	reply.Term = rf.currentTerm
	reply.Sucess = false

	if args.Term < rf.currentTerm {
		raftLog.Debug(
			logger.LT_Client,
			"%%%d received stale AppendEntries RPC from leader %%%d (stale term: %d) on term %d\n",
			rf.me, args.LeaderId, args.Term, rf.currentTerm,
		)
		return
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.eState = FOLLOWER
		raftLog.Debug(
			logger.LT_Client, "%%%d updated its term to %d\n", rf.me, rf.currentTerm,
		)
		rf.votedFor = args.LeaderId
	} else if rf.eState == CANDIDATE {
		// If AppendEntries RPC received from new leader: convert to follower
		rf.eState = FOLLOWER
		rf.votedFor = args.LeaderId
		raftLog.Debug(
			logger.LT_Candidate,
			"%%%d abstained from election on term %d\n",
			rf.me, rf.currentTerm,
		)
	}

	rf.hasReceivedMsg = true
	// rf.VotedFor = args.LeaderId

	// Rule 2: Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm
	// 首先检查Server存在与PrevLogIndex对应的日志条目
	if rf.logLength() <= args.PrevLogIndex {
		reply.XLen = rf.logLength()
		raftLog.Debug(
			logger.LT_Client,
			"%%%d received AppendEntries(HeartBeat? %v) from leader %%%d but failed: only has %d entries. PrevLogIndex %d is too long\n",
			rf.me,
			len(args.Entries) == 0,
			args.LeaderId,
			rf.logLength(),
			args.PrevLogIndex,
		)
		return
	}
	// 然后检查Leader和Follower之间的日志一致性。
	if args.PrevLogIndex < rf.snapshotIndex {
		// 检测到过时的 AppendEntries RPC。
		// 因为快照的生成(Snapshot())是由Client在日志条目被applied后触发的。
		// 这意味者snapshotIndex总是小于等于lastAppiled
		// 即snapshotIndex <= lastAppiled <= commitIndex
		// 而一个条目一旦被commited，就是安全的，所以快照中的条目不可能与leader的条目产生冲突
		// 因此不需要采取任何行动，之后检查下args.Entries中的条目是否需要加入日志即可
	} else if args.PrevLogIndex == rf.snapshotIndex {
		if rf.snapshotTerm != args.PrevLogTerm {
			// 不可能会不一样吧？
			raftLog.Error(
				logger.LT_Client,
				"%%%d inconsistency between snapshotTerm %d and PrevLogTerm %d on term %d\n",
				rf.me, rf.snapshotTerm, args.PrevLogTerm, rf.currentTerm,
			)
		}
	} else if rf.logAtIndex(args.PrevLogIndex).Term != args.PrevLogTerm {
		// 处理日志冲突
		// 1. 记录冲突条目的任期号为xTerm。
		// 2. 寻找xIndex，这是日志中第一个任期号为XTerm的条目的索引，或者说xTerm第一次出现的位置
		xIndex := args.PrevLogIndex
		reply.XTerm = rf.logAtIndex(xIndex).Term
		// 从当前索引向前搜索，直到找到不是XTerm的条目，或到达快照索引为止。
		// 因为快照中的元素已经稳定，不会存在与当前leader的日志冲突，故搜索下界为snapshotIndex(不包含)。
		// 如果没有找到不是XTerm的条目，则xIndex为snapshotIndex+1
		for ; xIndex-1 > rf.snapshotIndex; xIndex-- {
			if rf.logAtIndex(xIndex-1).Term != reply.XTerm {
				break
			}
		}
		reply.XIndex = xIndex
		reply.XLen = rf.logLength()
		raftLog.Debug(
			logger.LT_Client,
			"%%%d has entry[%d] conflicts with leader %%%d's [%d] at index %d on term %d\n",
			rf.me, rf.logAtIndex(args.PrevLogIndex).Term,
			args.LeaderId, args.PrevLogTerm, args.PrevLogIndex, rf.currentTerm,
		)
		return
	}

	if args.Entries == nil || len(args.Entries) == 0 {
		raftLog.Debug(
			logger.LT_Client,
			"%%%d received heartbeart(prev log index: %d) from leader %%%d on term %d\n",
			rf.me, args.PrevLogIndex, args.LeaderId, rf.currentTerm,
		)
	}

	// Append any new entries not already in the log
	offset := 0
	if args.PrevLogIndex < rf.snapshotIndex {
		// 调整偏移，使之后的entryIndex跳过快照
		offset = rf.snapshotIndex - args.PrevLogIndex
	}
	for ; offset < len(args.Entries); offset++ {
		entryIndex := args.PrevLogIndex + offset + 1
		if entryIndex >= rf.logLength() ||
			rf.logAtIndex(entryIndex).Term != args.Entries[offset].Term {
			// If an existing entry conflicts with a new one (same index but different terms),
			// delete the existing entry and all that follow it
			rf.log = rf.logSlice(0, entryIndex)
			// Append any new entries not already in the log
			rf.log = append(rf.log, args.Entries[offset:]...)
			raftLog.Info(
				logger.LT_Client,
				"%%%d successfully append entry %v to itself, now has %d entries, on term %d\n",
				rf.me, args.Entries[offset:], rf.logLength(), rf.currentTerm,
			)
			break
		}
	}

	reply.Sucess = true
	if args.LeaderCommit > rf.commitIndex && args.PrevLogIndex+len(args.Entries) > rf.commitIndex {
		// 这边需要加入一个额外的判断条件 args.PrevLogIndex + len(args.Entries) > rf.commitIndex
		// 假设此刻的情形如下，nextIndex[follower] = 4，Follower的commitIndex = 3，leader的commitIndex=4
		// Index:     1 2 3 4 5
		// Follower:  4 4 4 4
		// Leader:    4 4 4 5 6
		// 当Leader发送AppendEntries时会发生case 2的冲突，于是Leader将nextIndex[follower]调整为3
		// 就当Leader再次发送AppendEntries前，Leader发出了一个Heatbeat(PrevLogIndex=2, LeaderCommitIndex=4)
		// 这时就会出现错误，follower的commitIndex会变成2了
		originCommitIndex := rf.commitIndex
		rf.commitIndex = min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
		raftLog.Debug(
			logger.LT_Client,
			"%%%d follows its leader %%%d's commitIndex changing it from %d to %d on term %d\n",
			rf.me, args.LeaderId, originCommitIndex, rf.commitIndex, rf.currentTerm,
		)
		rf.cond.Broadcast()
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Set default reply
	reply.Term = rf.currentTerm

	// Rule 1: Reply immediately if term < currentTerm
	if args.Term < rf.currentTerm {
		raftLog.Debug(
			logger.LT_Snap,
			"%%%d received stale InstallSnapshot RPC(stale term: %d) on term %d\n",
			rf.me, args.Term, rf.currentTerm,
		)
		return
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.eState = FOLLOWER
		raftLog.Debug(
			logger.LT_Snap, "%%%d update its term to %d\n", rf.me, rf.currentTerm,
		)
		rf.votedFor = args.LeaderId
	}

	rf.hasReceivedMsg = true

	if args.Offset != 0 || args.Done != true {
		// The offset mechanism for splitting up the snapshot is not implemented.
		raftLog.Error(
			logger.LT_Snap,
			"%d: chunk is not implemented yet. Entire snapshot is sent in a single InstallSnapshot RPC\n",
			rf.me,
		)
		return
	}

	if args.LastIncludedIndex <= rf.snapshotIndex {
		// 如果收到的快照的索引不比当前快照新，则认为是过时的快照信息，直接返回
		raftLog.Trace(
			logger.LT_Snap,
			"%%%d received stale InstallSnapshot RPC(snapshot index: %d < %d) on term %d\n",
			rf.me, args.LastIncludedIndex, rf.snapshotIndex, rf.currentTerm,
		)
		return
	} else if rf.logLength()-1 < args.LastIncludedIndex {
		// 如果当前日志的最后索引小于快照的最后索引，说明快照包含未知的新信息
		// 用收到的快照替换整个日志
		originalIndex := rf.snapshotIndex
		rf.snapshotIndex = args.LastIncludedIndex
		rf.snapshotTerm = args.LastIncludedTerm
		rf.snapshot = args.Data
		rf.log = make(Log, 0)
		raftLog.Info(
			logger.LT_Snap,
			"%%%d updated its snapshot from index %d to %d on term %d\n",
			rf.me, originalIndex, args.LastIncludedIndex, rf.currentTerm,
		)
	} else {
		// 快照的最后索引处于当前日志范围内，可能包含未知的新信息
		// 因为 rf.snapshotIndex < args.LastIncludedIndex < rf.logLength()
		// 所以rf.logAtIndex(args.LastIncludedIndex)不会越界
		if rf.logAtIndex(args.LastIncludedIndex).Term != args.LastIncludedTerm {
			// 如果现有日志中相同索引处的条目任期与快照不同，则用收到的快照替换整个日志。
			originalLogLength := rf.logLength()
			originalSnapshotIndex := rf.snapshotIndex
			rf.snapshotIndex = args.LastIncludedIndex
			rf.snapshotTerm = args.LastIncludedTerm
			rf.snapshot = args.Data
			rf.log = make(Log, 0)
			raftLog.Info(
				logger.LT_Snap,
				"%%%d discarded entrie log(length %d) and created snapshot(index %d -> %d) on term %d\n",
				rf.me, originalLogLength, originalSnapshotIndex, args.LastIncludedIndex, rf.currentTerm,
			)
		} else {
			// Rule 6: If existing log entry has same index and term as snapshot’s
			//         last included entry, retain log entries following it and reply
			raftLog.Info(
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
	rf.applyCh <- msg
	raftLog.Info(
		logger.LT_APPLIER, "%%%d applied <Snapshot Index %d, Snapshot Term %d> on term %d\n",
		rf.me, msg.SnapshotIndex, msg.SnapshotTerm, rf.currentTerm,
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

// sendHeartBeat 向指定的服务器发送心跳消息。这个心跳实际上是一个空的AppendEntries RPC
// 心跳的发送有助于保持领导者的权威并防止Follower超时转换为Candidate
func (rf *Raft) sendHeartBeat(server int, sentTerm int) {
	rf.mu.Lock()
	if rf.eState != LEADER {
		rf.mu.Unlock()
		return
	}
	raftLog.Trace(
		logger.LT_Leader, "%%%d attempt to send heartbeat to %%%d on term %d\n",
		rf.me, server, sentTerm,
	)
	prevLogIndex := rf.snapshotIndex
	prevLogTerm := rf.snapshotTerm
	if rf.nextIndex[server]-1 > rf.snapshotIndex {
		// NOTE
		// 如果follower的日志落后于leader的snapshot，那么使用snapshotIndex作为prevLogIndex
		// 这会导致follower在处理AppendEntries RPC时，发生冲突然后直接返回，
		// 尽管接受到了Heartbeat，但是不会打印相关信息
		prevLogIndex = rf.nextIndex[server] - 1
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
	ok := rf.sendAppendEntries(server, &args, &reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ok {
		raftLog.Debug(
			logger.LT_Leader,
			"%%%d cannot send heartbeat(sent on term %d) to %d on term %d\n",
			rf.me, sentTerm, server, rf.currentTerm,
		)
		return
	}
	raftLog.Debug(
		logger.LT_Leader,
		"%%%d sent heartbeat to server %%%d on term %d, received reply on term %d\n",
		rf.me, server, sentTerm, rf.currentTerm,
	)
	if reply.Term > rf.currentTerm {
		rf.eState = FOLLOWER
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		raftLog.Debug(
			logger.LT_Client, "%%%d update its term to %d and converted to follower\n",
			rf.me, rf.currentTerm,
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
		sendTerm := rf.currentTerm
		rf.mu.Unlock()
		for i := range rf.peers { // send heartbeat to each server
			if i != rf.me {
				go rf.sendHeartBeat(i, sendTerm)
			}
		}
		time.Sleep(MS_HEARTBEAT_INTERVAL * time.Millisecond)
	}
}

// 感觉lastLogIndex 这个参数可能是多余的
func (rf *Raft) replicate(
	client int, replicateTerm int, lastLogIndex int, finishedClient chan int,
) {
	rf.mu.Lock()
	if rf.nextIndex[client] > lastLogIndex {
		raftLog.Debug(
			logger.LT_Leader,
			"%%%d's lastLogIndex %d < client %%%d's nextIndex %d when replicate\n",
			rf.me, lastLogIndex, client, rf.nextIndex[client],
		)
		rf.mu.Unlock()
		return
	} else if rf.eState != LEADER {
		raftLog.Trace(logger.LT_Client, "%%%d is not leader and stop replicating on term %d\n", rf.me, rf.currentTerm)
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
			// 如果follower落后于leader的snapshot，更新follower的快照
			rf.mu.Unlock()
			if rf.updateFollowerSnapshot(client, replicateTerm) == false {
				raftLog.Trace(
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
		entries := make(Log, rf.logLength()-prevLogIndex-1)
		copy(entries, rf.logSlice(prevLogIndex+1, -1))
		args := AppendEntriesArgs{
			Term:         replicateTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}
		reply := AppendEntriesReply{}
		raftLog.Debug(
			logger.LT_Leader,
			"%%%d attempt to replicate entries %v to %%%d (Prev- Index: %d, Term: %d)on term %d\n",
			rf.me, args.Entries, client, prevLogIndex, prevLogTerm, replicateTerm,
		)
		rf.mu.Unlock()

		ok := rf.sendAppendEntries(client, &args, &reply)
		rf.mu.Lock()
		if rf.killed() || rf.eState != LEADER {
			raftLog.Trace(
				logger.LT_Leader,
				"%%%d stop sendAppendEntries(killed: %v, leader: %v) on term %d\n",
				rf.me, rf.killed(), rf.eState == LEADER, rf.currentTerm,
			)
			break
		} else if ok {
			if reply.Sucess {
				// Q: 这边matchIndex的计算为什么要取max呢？
				// A: 假设目前 log = [nil, 1], matchIndex = 1, nextIndex = 2
				// 然后client请求执行cmd 2，于是leader将2加入log，log = [nil, 1, 2]
				// 然后leader发出replicate A，replicate [2]
				// 但是在A得到回复前，client请求执行cmd 3，于是leader将3加入log，log = [nil, 1, 2, 3]
				// 并向client发出replicate B， replicate [2, 3]
				// 如果B先于A执行，执行后 matchIndex = 3， nextIndex = 4
				// 之后再执行A，这时候我们保持matchIndex和nextIndex不变就好
				rf.matchIndex[client] = max(rf.matchIndex[client], prevLogIndex+len(args.Entries))
				rf.nextIndex[client] = rf.matchIndex[client] + 1
				raftLog.Info(
					logger.LT_Leader,
					"%%%d succeeded in replicating entry %v to %%%d(sent on term %d) on term %d\n",
					rf.me, args.Entries, client, replicateTerm, rf.currentTerm,
				)
				// 用于leader统计有几个server完成了replicate
				finishedClient <- client
				break
			} else {
				if rf.currentTerm < reply.Term {
					rf.currentTerm = reply.Term
					rf.eState = FOLLOWER
					rf.votedFor = -1
					raftLog.Debug(
						logger.LT_Leader, "%%%d updated its term to %d and converted to follower\n",
						rf.me, rf.currentTerm,
					)
					rf.persist()
					break
				} else if rf.currentTerm > replicateTerm { // Simply drop the RPC response from the old term
					raftLog.Trace(
						logger.LT_Leader,
						"%%%d drop an old appendEntries response(sent on term %d) on current term %d\n",
						rf.me, replicateTerm, rf.currentTerm,
					)
					break
				}

				rf.nextIndex[client] = rf.fastbackup(&args, &reply)
				raftLog.Debug(
					logger.LT_Leader,
					"%%%d: AppendEntries to %%%d failed(sent on term %d) because of inconsistency on term %d, retry again\n",
					rf.me, client, replicateTerm, rf.currentTerm,
				)
			}
		} else { // RPC failed
			raftLog.Debug(
				logger.LT_Leader,
				"%%%d: AppendEntries(sent on term %d) to %%%d failed, try again\n",
				rf.me, replicateTerm, client,
			)
		}
	}
	rf.mu.Unlock()
}

func (rf *Raft) fastbackup(args *AppendEntriesArgs, reply *AppendEntriesReply) int {
	// Fast Backup https://www.youtube.com/watch?v=4r8Mz3MMivY&t=3655s
	if reply.XLen <= args.PrevLogIndex {
		// Case 3: follower's log is too short:
		//   nextIndex = XLen
		raftLog.Trace(
			logger.LT_Leader, "%d fast backup case 3 - follower's log is too short, only %d\n",
			rf.me, reply.XLen,
		)
		return reply.XLen
	} else if lastEntryIndex := rf.hasTerm(reply.XTerm, args.PrevLogIndex); lastEntryIndex == -1 {
		// Case 1: leader doesn't have XTerm:
		//   nextIndex = XIndex
		raftLog.Trace(
			logger.LT_Leader,
			"%d fast backup case 1 - leader doesn't have the term %d and next index will be %d\n",
			rf.me, reply.XTerm, reply.XIndex,
		)
		return reply.XIndex
	} else {
		// Case 2: leader has XTerm:
		//   nextIndex = leader's last entry for XTerm
		raftLog.Trace(
			logger.LT_Leader, "%d fast backup case 2 - leadder has XTerm %d and its last index is %d\n",
			rf.me, reply.XTerm, lastEntryIndex,
		)
		// 感觉改为rf.nextIndex[client] = lastEntryIndex + 1也行？
		// rf.nextIndex[client] = lastEntryIndex
		return lastEntryIndex
	}
}

func (rf *Raft) updateFollowerSnapshot(client int, replicateTerm int) bool {
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
	rf.mu.Unlock()

	for {
		rf.mu.Lock()
		reply := InstallSnapshotReply{}
		if rf.killed() || rf.eState != LEADER {
			raftLog.Trace(
				logger.LT_Snap,
				"%%%d stop sendInstallSnapshot(killed: %v, leader: %v) on term %d\n",
				rf.me, rf.killed(), rf.eState == LEADER, rf.currentTerm,
			)
			rf.mu.Unlock()
			return false
		}
		raftLog.Debug(
			logger.LT_Leader,
			"%%%d attempt to InstallSnapshot(Index %d, Term %d) to %%%d on term %d\n",
			rf.me, args.LastIncludedIndex, args.LastIncludedTerm, client, rf.currentTerm,
		)
		rf.mu.Unlock()

		if rf.sendInstallSnapshot(client, &args, &reply) == true {
			rf.mu.Lock()
			if rf.currentTerm < reply.Term {
				rf.currentTerm = reply.Term
				rf.eState = FOLLOWER
				rf.votedFor = client
				raftLog.Debug(
					logger.LT_Snap, "%%%d updated its term to %d and converted to follower\n",
					rf.me, rf.currentTerm,
				)
				rf.persist()
				rf.mu.Unlock()
				return false
			} else if rf.currentTerm > replicateTerm { // Simply drop the RPC response from the old term
				raftLog.Trace(
					logger.LT_Leader,
					"%%%d drop an stale sendInstallSnapshot response(sent on term %d) on current term %d\n",
					rf.me, replicateTerm, rf.currentTerm,
				)
				rf.mu.Unlock()
				return false
			} else {
				raftLog.Info(
					logger.LT_Snap,
					"%%%d succeeded in InstallSnapshot(Index %d, Term %d) to %%%d on term %d\n",
					rf.me, args.LastIncludedIndex, args.LastIncludedTerm, client, rf.currentTerm,
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
	rf.currentTerm++
	rf.eState = CANDIDATE
	electionTerm := rf.currentTerm

	lastLogIndex := rf.snapshotIndex
	lastLogTerm := rf.snapshotTerm
	if len(rf.log) != 0 {
		lastLogIndex = rf.logLength() - 1
		lastLogTerm = rf.logAtIndex(lastLogIndex).Term
	}
	raftLog.Info(
		logger.LT_Vote, "%%%d starts an election on term %d\n", rf.me, electionTerm,
	)
	rf.voteCount++ // Vote for itself
	rf.votedFor = rf.me
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
			raftLog.Trace(
				logger.LT_Vote, "%%%d request %%%d to vote on term %d\n",
				rf.me, n, electionTerm,
			)
			for {
				ok := rf.sendRequestVote(n, &args, &reply)
				if rf.killed() {
					raftLog.Trace(
						logger.LT_Vote, "%%%d stop request vote since it has been killed\n", rf.me,
					)
					return
				} else if ok {
					break
				} else {
					rf.mu.Lock()
					if electionTerm != rf.currentTerm {
						raftLog.Trace(logger.LT_Vote, "%%%d cancel an old vote requst(sent on term %d) to %%%d\n", rf.me, electionTerm, n)
					}
					rf.mu.Unlock()
					return
				}
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()
			raftLog.Trace(
				logger.LT_Vote, "%%%d get reply from %%%d(vote for me: %v) on term %d\n",
				rf.me, n, reply.VoteGranted, rf.currentTerm,
			)
			if !reply.VoteGranted { // Get a rejection
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
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
				if electionTerm != rf.currentTerm {
					raftLog.Debug(
						logger.LT_Vote, "%%%d: a belated win(start at term %d, now on term %d)\n",
						rf.me, electionTerm, rf.currentTerm,
					)
					return
				}
				rf.eState = LEADER
				for j := range rf.peers {
					rf.nextIndex[j] = rf.logLength()
					rf.matchIndex[j] = 0
				}
				raftLog.Info(
					logger.LT_Vote, "%%%d won election on term %d\n", rf.me, rf.currentTerm,
				)

				go rf.heartBeatSender()
			}
		}(i)

	}

	return true
}

func (rf *Raft) commit(replicateTerm int, lastLogIndex int) {
	// Replicate
	// 通过使用有缓冲的channel，避免了当replicateCount达到majority时，
	// 部分gorouting无法向channel写入数据而导致的死锁
	finishedClient := make(chan int, len(rf.peers)-1)
	for i := range rf.peers {
		if i != rf.me {
			go rf.replicate(i, replicateTerm, lastLogIndex, finishedClient)
		}
	}

	// 等待，直到在majority上完成replicate
	replicateCount := 1
	majority := len(rf.peers)/2 + 1
	for replicateCount < majority {
		client := <-finishedClient
		replicateCount++
		raftLog.Trace(
			logger.LT_Commit,
			"%%%d was notified a replicate to %%%d finished(total finished count %d)\n",
			rf.me, client, replicateCount,
		)
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	raftLog.Trace(
		logger.LT_Commit, "%%%d get enough replication count %d on term %d\n",
		rf.me, replicateCount, rf.currentTerm,
	)
	// commitIndex+1  commitIndex+2 ... len(rf.log)-1
	// 0              1             ... len(count)-1
	var i int
	countLength := rf.logLength() - 1 - rf.commitIndex
	if countLength == 0 {
		raftLog.Trace(
			logger.LT_Commit, "%%%d: stale command committed on term %d\n",
			rf.me, rf.currentTerm,
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
	raftLog.Trace(logger.LT_Commit, "%%%d replic count: %v\n", rf.me, count)
	for i = countLength - 1; i >= 0; i-- {
		if count[i] >= majority && rf.logAtIndex(rf.commitIndex+1+i).Term == rf.currentTerm {
			newCommitIndex := rf.commitIndex + 1 + i
			raftLog.Debug(
				logger.LT_Leader, "%%%d update commitIndex(from %d to %d) on term %d\n",
				rf.me, rf.commitIndex, newCommitIndex, rf.currentTerm,
			)
			rf.commitIndex = newCommitIndex
			rf.cond.Broadcast()
			break
		}
	}
}

// applier 负责应用(apply)已提交的(commmitted)日志条目到状态机。
// 在唤醒后，它会持续检查是否有新的可以应用的日志条目，并在有新条目时将其发送到 applyCh。
// 如果没有则wait，等待下一次唤醒
func (rf *Raft) applier() {
	rf.mu.Lock()
	for {
		if rf.killed() {
			close(rf.applyCh)
			break
		} else if rf.lastAppiled < rf.commitIndex {
			for {
				if rf.lastAppiled >= rf.logLength() {
					raftLog.Error(
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
				raftLog.Info(
					logger.LT_APPLIER, "%%%d applied <Command Index: %d, Command %v> on term %d\n",
					rf.me, msg.CommandIndex, msg.Command, rf.currentTerm,
				)
				if rf.lastAppiled == rf.commitIndex {
					break
				}
			}
		} else if rf.lastAppiled > rf.commitIndex {
			raftLog.Error(
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
			// Append entry to local log
			rf.log = append(rf.log, logEntry{Term: rf.currentTerm, Command: command})
			rf.persist()
			replicateTerm := rf.currentTerm
			lastLogIndex := rf.logLength() - 1
			index = lastLogIndex
			raftLog.Info(
				logger.LT_Commit, "%%%d received command %v at index %v on term %d\n",
				rf.me, command, index, rf.currentTerm,
			)
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
	// raftLog.Warn(logger.LT_Log, "%%%d killed\n", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.
		// raftLog.Trace(logger.LT_Timer, "%%%d attempt tick\n", rf.me)
		rf.mu.Lock()
		raftLog.Trace(logger.LT_Timer, "%%%d tick on term %d\n", rf.me, rf.currentTerm)
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
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.hasReceivedMsg = false
	rf.voteCount = 0
	rf.eState = FOLLOWER

	// Q: Why is the Raft log 1-indexed?
	// A: You should view it as zero-indexed, but starting out with an entry
	// (at index=0) that has term 0. That allows the very first AppendEntries
	// RPC to contain 0 as PrevLogIndex, and be a valid index into the log.
	rf.log = make(Log, 0)
	rf.log = append(rf.log, logEntry{Term: 0})

	rf.cond = *sync.NewCond(&rf.mu)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// snapshotIndex被初始化为-1， 因此需要用max来保证
	// commitIndex和lastApplied>=0
	rf.commitIndex = max(0, rf.snapshotIndex)
	rf.lastAppiled = max(0, rf.snapshotIndex)
	raftLog.Info(logger.LT_Log, "%%%d had been made\n", rf.me)

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applier()

	return rf
}
