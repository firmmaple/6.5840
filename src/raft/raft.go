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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

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
	currentTerm    int // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor       int // candidateId that received vote in current term (or -1 if none)
	hasReceivedMsg bool
	voteCount      int
	eState         electionState

	log         []logEntry // log entries
	index       int
	commitIndex int   // index of highest log entry known to be committed (initialized to 0)
	lastAppiled int   // index of highest log entry applied to state machine (initialized to 0)
	nextIndex   []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex  []int // for each server, index of highest log entry known to be replicated on server (initialized to 0)
	cond        sync.Cond
	applyCh     chan ApplyMsg
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

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
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
		logger.Debug(
			logger.LT_Vote,
			"%%%d term(%d) is more update compare to candidate %%%d's term(%d)\n",
			rf.me, rf.currentTerm, args.CandidateID, args.Term,
		)
		return
	} else if args.Term > rf.currentTerm {
		rf.eState = FOLLOWER
		rf.votedFor = -1
		rf.currentTerm = args.Term
		logger.Debug(logger.LT_Vote, "%%%d converted to follower on term %d\n", rf.me, rf.currentTerm)
	}

	if rf.votedFor != -1 && rf.votedFor != args.CandidateID {
		logger.Debug(
			logger.LT_Vote,
			"%%%d refused to vote for candidate %%%d(it has already voted for %%%d) on term %d\n",
			rf.me, args.CandidateID, rf.votedFor, rf.currentTerm,
		)
		return
	}
	// Server A is more update than B if
	// 1. A has higger term in last entry,
	// or
	// 2. they have the same term, but A's log is longer the B
	lastLogTerm := rf.log[len(rf.log)-1].Term
	if args.LastLogTerm > lastLogTerm {
		// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
		rf.votedFor = args.CandidateID
		reply.VoteGranted = true
		rf.hasReceivedMsg = true
		logger.Debug(
			logger.LT_Vote, "%%%d vote candidate %%%d on term %d\n",
			rf.me, args.CandidateID, rf.currentTerm,
		)
	} else if args.LastLogTerm == lastLogTerm && args.LastLogIndex+1 >= len(rf.log) {
		// if rf.eState == LEADER {
		// 	logger.Debug(
		// 		logger.LT_Vote, "%%%d refused to vote for candidate %%%d(it has already been leader) on term %d\n",
		// 		rf.me, args.CandidateID, rf.currentTerm,
		// 	)
		// 	return
		// }
		reply.VoteGranted = true
		rf.hasReceivedMsg = true
		logger.Debug(logger.LT_Vote, "%%%d vote for candidate %%%d(Repeat Vote=%v) on term %d\n",
			rf.me, args.CandidateID, rf.votedFor == args.CandidateID, rf.currentTerm)
		rf.votedFor = args.CandidateID
	} else {
		logger.Debug(logger.LT_Vote, "%%%d refused to vote for candidate %%%d since itself is more update on term %d\n",
			rf.me, args.CandidateID, rf.currentTerm)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Set default reply
	reply.Term = rf.currentTerm
	reply.Sucess = false

	if args.Term < rf.currentTerm {
		logger.Debug(
			logger.LT_Client,
			"%%%d's term(%d) is bigger than Leader %%%d's term(%d) in RPC AppendEntries\n",
			rf.me, rf.currentTerm, args.LeaderId, args.Term,
		)
		return
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.eState = FOLLOWER
		logger.Debug(
			logger.LT_Client, "%%%d update its term to %d\n", rf.me, rf.currentTerm,
		)
		rf.votedFor = args.LeaderId
	} else if rf.eState == CANDIDATE {
		// If AppendEntries RPC received from new leader: convert to follower
		rf.eState = FOLLOWER
		rf.votedFor = args.LeaderId
		logger.Debug(
			logger.LT_Candidate,
			"%%%d abstained from election on term %d\n",
			rf.me, rf.currentTerm,
		)
	}

	if rf.votedFor == args.LeaderId {
		rf.hasReceivedMsg = true
	} else {
		logger.Info(logger.LT_Client, "%%%d received AppendEntries from non-current leader %%%d\n", rf.me, args.LeaderId)
	}

	// Check for log consistency between leader and follower
	if len(rf.log) <= args.PrevLogIndex {
		logger.Debug(
			logger.LT_Client,
			"%%%d only has %d entries. PrevLogIndex %d is too long\n",
			rf.me, len(rf.log), args.PrevLogIndex,
		)
		return
	}
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		logger.Debug(
			logger.LT_Client,
			"%%%d has entry[%d] conflits with leader %%%d's [%d] at index %d on term %d\n",
			rf.me, rf.log[args.PrevLogIndex].Term,
			args.LeaderId, args.PrevLogTerm, args.PrevLogIndex, rf.currentTerm,
		)
		return
	}

	if args.Entries == nil || len(args.Entries) == 0 {
		logger.Debug(logger.LT_Client, "%%%d received heartbeart from leader %%%d on term %d\n",
			rf.me, args.LeaderId, rf.currentTerm)
	}

	// Append any new entries not already in the log
	for offset := 0; offset < len(args.Entries); offset++ {
		entryIndex := args.PrevLogIndex + offset + 1
		if len(rf.log) <= entryIndex || rf.log[entryIndex].Term != args.Entries[offset].Term {
			// If an existing entry conflicts with a new one (same index but different terms),
			// delete the existing entry and all that follow it
			rf.log = rf.log[:entryIndex]
			// Append any new entries not already in the log
			// logger.Warn(logger.LT_Log, "%d log len %d\n", rf.me, len(rf.log))
			rf.log = append(rf.log, args.Entries[offset:]...)
			// logger.Warn(logger.LT_Log, "%d log len %d\n", rf.me, len(rf.log))
			logger.Info(
				logger.LT_Client,
				"%%%d successfully append entry %v to itself, now has %d entries, on term %d\n",
				rf.me, args.Entries[offset:], len(rf.log), rf.currentTerm,
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
			rf.me, args.LeaderId, originCommitIndex, rf.commitIndex, rf.currentTerm,
		)
		rf.cond.Broadcast()
	}
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
	server int,
	args *AppendEntriesArgs,
	reply *AppendEntriesReply,
) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	return ok
}

func (rf *Raft) sendHeartBeat(client int) {
	rf.mu.Lock()
	if rf.killed() || rf.eState != LEADER {
		rf.mu.Unlock()
		return
	}
	sentTerm := rf.currentTerm
	logger.Debug(
		logger.LT_Leader, "%%%d attempt to send heartbeat to %%%d on term %d\n",
		rf.me, client, sentTerm,
	)
	preLogIndex := rf.nextIndex[client] - 1
	args := AppendEntriesArgs{
		Term:         sentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: preLogIndex,
		PrevLogTerm:  rf.log[preLogIndex].Term,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()
	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(client, &args, &reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ok {
		if sentTerm < rf.currentTerm {
			logger.Trace(
				logger.LT_Leader,
				"%%%d heartbeat sent on term %d to %d failed\n",
				rf.me, client, sentTerm,
			)
		} else {
			logger.Debug(
				logger.LT_Leader,
				"%%%d cannot sent heartbeat to %d on term %d\n",
				rf.me, client, rf.currentTerm,
			)
		}
		return
	}
	logger.Info(
		logger.LT_Leader,
		"%%%d sent heartbeat to server %%%d on term %d, received reply on term %d\n",
		rf.me, client, sentTerm, rf.currentTerm,
	)
	if reply.Term > rf.currentTerm {
		rf.eState = FOLLOWER
		rf.currentTerm = reply.Term
		logger.Debug(
			logger.LT_Client, "%%%d update its term to %d and convert to follower\n",
			rf.me, rf.currentTerm,
		)
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
	client int,
	replicateTerm int,
	lastLogIndex int,
	finishedClient chan int,
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

	prevLogIndex := rf.nextIndex[client] - 1
	// 如果不采用深拷贝，可能会产生data race
	// RPC调用时读取args的Entries没有锁，这时候如果其他goroutine在写的话，会产生data race
	entries := make([]logEntry, len(rf.log)-prevLogIndex-1)
	copy(entries, rf.log[prevLogIndex+1:])
	args := AppendEntriesArgs{
		Term:         replicateTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  rf.log[prevLogIndex].Term,
		Entries:      entries,
	}
	reply := AppendEntriesReply{}
	rf.mu.Unlock()

	logger.Trace(
		logger.LT_Leader, "%%%d attempt to replicate entries %v to %%%d on term %d\n",
		rf.me, args.Entries, client, replicateTerm,
	)
	for {
		ok := rf.sendAppendEntries(client, &args, &reply)
		rf.mu.Lock()
		if rf.killed() || rf.eState != LEADER {
			logger.Trace(
				logger.LT_Leader,
				"%%%d stop sendAppendEntries(killed: %v, leader: %v) on term %d\n",
				rf.me,
				rf.killed(),
				rf.eState == LEADER,
				rf.currentTerm,
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
				logger.Trace(
					logger.LT_Leader,
					"%%%d succeeded in replicating entry %v to %%%d(sent on term %d) on term %d\n",
					rf.me, args.Entries, client, replicateTerm, rf.currentTerm,
				)
				rf.mu.Unlock()
				break
			} else {
				if rf.currentTerm < reply.Term {
					rf.currentTerm = reply.Term
					rf.eState = FOLLOWER
					logger.Debug(
						logger.LT_Leader,
						"%%%d update its term to %d and convert to follower\n",
						rf.me, rf.currentTerm,
					)
					rf.mu.Unlock()
					return
				} else if rf.currentTerm > replicateTerm { // Simply drop the RPC response from the old term
					logger.Trace(
						logger.LT_Leader,
						"%%%d drop an old appendEntries response(sent on term %d) on current term %d\n",
						rf.me, replicateTerm, rf.currentTerm,
					)
					rf.mu.Unlock()
					return
				}
				rf.nextIndex[client]--
				prevLogIndex--
				args.PrevLogIndex = prevLogIndex
				args.PrevLogTerm = rf.log[prevLogIndex].Term
				args.Entries = append([]logEntry{rf.log[prevLogIndex+1]}, args.Entries...)
				logger.Debug(
					logger.LT_Leader,
					"%%%d: AppendEntries to %%%d failed(sent on term %d) beacause of inconsistency on term %d, retry again\n",
					rf.me, client, replicateTerm, rf.currentTerm,
				)
			}
		} else { // RPC failed
			logger.Trace(
				logger.LT_Leader,
				"%%%d: AppendEntries(sent on term %d) to %%%d failed, try again\n",
				rf.me, replicateTerm, client,
			)
		}
		rf.mu.Unlock()
	}

	// rf.cond.Broadcast()
	finishedClient <- client
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
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term
	logger.Info(
		logger.LT_Vote, "%%%d starts an election on term %d\n", rf.me, electionTerm,
	)
	rf.voteCount++ // Vote for itself
	rf.votedFor = rf.me
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
					if electionTerm != rf.currentTerm {
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
				logger.LT_Vote, "%%%d: get reply from %%%d(vote for me: %v) on term %d\n",
				rf.me, n, reply.VoteGranted, rf.currentTerm,
			)
			if !reply.VoteGranted { // Get a rejection
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.eState = FOLLOWER
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
					logger.Warn(
						logger.LT_Vote, "%%%d: a belated win(start at term %d, now on term %d)",
						rf.me, electionTerm, rf.currentTerm,
					)
					return
				}
				rf.eState = LEADER
				rf.votedFor = -1
				for j := range rf.peers {
					rf.nextIndex[j] = len(rf.log)
					rf.matchIndex[j] = 0
				}
				logger.Info(
					logger.LT_Vote, "%%%d won election on term %d\n", rf.me, rf.currentTerm,
				)

				go rf.heartBeatSender()
			}
		}(i)
	}

	return true
}

func (rf *Raft) commit(replicateTerm int, lastLogIndex int) {
	// if _, isLeader := rf.GetState(); isLeader == false { // Just in case
	// 	logger.Fatalln(logger.LT_Leader, "Only leader has right to execute command")
	// }

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
		rf.me, replicateCount, rf.currentTerm,
	)
	// commitIndex+1  commitIndex+2 ... len(rf.log)-1
	// 0              1             ... len(count)-1
	var i int
	countLength := len(rf.log) - 1 - rf.commitIndex
	if countLength == 0 {
		logger.Trace(
			logger.LT_Commit, "%%%d: stale command committed on term %d\n",
			rf.me, rf.currentTerm,
		)
		return
	}
	count := make([]int, countLength)
	for i = range rf.peers {
		if i == rf.me {
			count[len(count)-1]++
		} else {
			index := rf.matchIndex[i] - rf.commitIndex - 1
			if index >= 0 {
				count[index]++
			}
		}
	}
	logger.Trace(logger.LT_Commit, "%%%d replic count: %v\n", rf.me, count)
	for i = countLength - 1; i >= 0; i-- {
		if count[i] >= majority && rf.log[rf.commitIndex+1+i].Term == rf.currentTerm {
			newCommitIndex := rf.commitIndex + 1 + i
			logger.Debug(
				logger.LT_Leader, "%%%d update commitIndex(from %d to %d) on term %d\n",
				rf.me, rf.commitIndex, newCommitIndex, rf.currentTerm,
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
				if rf.lastAppiled >= len(rf.log) {
					logger.Error(
						logger.LT_APPLIER, "%%%d: index(lastAppiled %d) out of range(log length %d)\n",
						rf.me, rf.lastAppiled, len(rf.log),
					)
				}
				rf.lastAppiled++
				msg := ApplyMsg{
					CommandValid: true,
					Command:      rf.log[rf.lastAppiled].Command,
					CommandIndex: rf.lastAppiled,
				}
				rf.applyCh <- msg
				logger.Info(
					logger.LT_APPLIER, "%%%d applyMsg %v on term %d\n", rf.me, msg, rf.currentTerm,
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
			logger.Info(logger.LT_Commit, "%%%d received command %v\n", rf.me, command)
			// Append entry to local log
			rf.log = append(rf.log, logEntry{Term: rf.currentTerm, Command: command})
			replicateTerm := rf.currentTerm
			lastLogIndex := len(rf.log) - 1
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
		logger.Trace(logger.LT_Timer, "%%%d tick on term %d\n", rf.me, rf.currentTerm)
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
	rf.log = make([]logEntry, 0)
	rf.log = append(rf.log, logEntry{Term: 0})
	rf.index = 1

	rf.cond = *sync.NewCond(&rf.mu)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	logger.Info(logger.LT_Log, "%%%d had been made\n", rf.me)

	// start ticker goroutine to start elections
	go rf.ticker()

	rf.commitIndex = 0
	rf.lastAppiled = 0
	go rf.applier()

	return rf
}
