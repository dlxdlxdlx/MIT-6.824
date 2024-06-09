package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log Entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}
type LogEntry struct {
	term int
	data string
}

type logTopic string

const (
	dClient  logTopic = "CLNT"
	dCommit  logTopic = "CMIT"
	dDrop    logTopic = "DROP"
	dError   logTopic = "ERRO"
	dInfo    logTopic = "INFO"
	dLeader  logTopic = "LEAD"
	dLog     logTopic = "LOG1"
	dLog2    logTopic = "LOG2"
	dPersist logTopic = "PERS"
	dSnap    logTopic = "SNAP"
	dTerm    logTopic = "TERM"
	dTest    logTopic = "TEST"
	dTimer   logTopic = "TIMR"
	dTrace   logTopic = "TRCE"
	dVote    logTopic = "VOTE"
	dWarn    logTopic = "WARN"
)
const (
	heartbeatTimeout   = 100
	minElectionTimeout = 150
	maxElectionTimeout = 250
)

type Role string

// Raft A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	appCh          chan ApplyMsg
	applyCond      *sync.Cond
	replicatorCond []*sync.Cond
	state          Role

	currentTerm int
	votedFor    int
	Entries     []LogEntry

	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
}

func (rf *Raft) BroadcastHeartBeat() {
	DPrintf(dLeader, "S%v send AppendEntries ", rf.me)
	rf.mu.Lock()
	request := &AppendEntriesArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		Entries:           []LogEntry{},
		LeaderCommitIndex: rf.commitIndex,
	}
	if len(rf.Entries) == 0 {
		request.PrevLogTerm = -1
		request.PrevLogIndex = -1
	} else {
		request.PrevLogIndex = rf.Entries[len(rf.Entries)].term
		request.PrevLogIndex = len(rf.Entries) - 1
	}
	rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		response := &AppendEntriesReply{}
		DPrintf(dLeader, "S%v send AppendEntries to %v.", rf.me, i)
		go func(peer int) {
			for ok := rf.peers[peer].Call("Raft.AppendEntries", request, response); !ok; {

			}
		}(i)
	}
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	return rf.currentTerm, rf.state == "leader"
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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

// CondInstallSnapshot A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// RequestVoteArgs example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //candidate's Term
	CandidateId  int // candidate requesting vote
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term         int
	VotedGranted bool
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(request *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//DPrintf("{S %v}'s state: {state: %v, Term: %v, commitIndex: %v, lastApplied: %v}", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if request.Term < rf.currentTerm || (request.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != request.CandidateId) {
		reply.Term, reply.VotedGranted = rf.currentTerm, false
		return
	}
	if request.Term > rf.currentTerm {
		rf.state = "follower"
		rf.currentTerm, rf.votedFor = request.LastLogTerm, -1
	}
	if rf.Entries[len(rf.Entries)-1].term > request.LastLogTerm || (rf.Entries[len(rf.Entries)-1].term == request.LastLogTerm && len(rf.Entries) > request.LastLogIndex) {
		reply.Term, reply.VotedGranted = rf.currentTerm, false
	}
	rf.electionTimer.Reset(RandomElectionDuration())
	DPrintf(dClient, "S%v vote for S%v", rf.me, request.CandidateId)
	reply.Term, reply.VotedGranted = rf.currentTerm, true
	return
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
// 核心是通过调用Call命令来模拟网络数据传输过程中可能出现的异常状态.
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

// Start the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// Kill the tester doesn't halt goroutines created by Raft after each test,
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select {
		case <-rf.electionTimer.C:
			//DPrintf("S{%v} electionTimeout", rf.me)
			DPrintf(dTimer, "S%v electionout", rf.me)
			rf.mu.Lock()
			if rf.state != "leader" {
				rf.state = "candidate"
				rf.currentTerm += 1
				rf.StartElection()
			} else {
				rf.electionTimer.Reset(RandomElectionDuration())
			}
			rf.mu.Unlock()
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.state == "leader" {
				DPrintf(dLeader, "S%v heartBeatTimeout", rf.me)
				rf.BroadcastHeartBeat()
			}
			rf.heartbeatTimer.Reset(heartbeatTimeout * time.Millisecond)
			rf.mu.Unlock()
		}

	}
}

type AppendEntriesArgs struct {
	Term              int        //leader's Term
	LeaderId          int        //leader id
	PrevLogIndex      int        // index of log entry immediately precceding new ones
	PrevLogTerm       int        // Term of PrevLogIndex entry
	Entries           []LogEntry //log Entries to store(empty for heartBeat; may send more than one for efficiency)
	LeaderCommitIndex int        //leader's commitIndex
}
type AppendEntriesReply struct {
	Term       int //currentTerm, for leader to update itself
	MatchIndex int
	Success    bool //true if follower contained entry matching PrevLogIndex and PrevLogTerm
}

// AppendEntries 添加日志条目, 同时也可以作为心跳机制RPC调用
func (rf *Raft) AppendEntries(request *AppendEntriesArgs, response *AppendEntriesReply) {
	DPrintf(dClient, "S%v recv AppendEntries from S%v S%v", rf.me, request.LeaderId)
	rf.mu.Lock()
	defer rf.mu.Lock()
	//1. Reply false if term < currentTerm (§5.1)
	if request.Term < rf.currentTerm {
		response.Term, response.Success = rf.currentTerm, false
		return
	}
	if request.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = request.Term, -1
	}
	if rf.state != "follower" && rf.currentTerm <= request.Term {
		//选举还没有完成就收到心跳包的话就自动变成follower
		rf.state = "follower"
	}
	rf.electionTimer.Reset(RandomElectionDuration())
	// 如果是心跳包的话
	if len(request.Entries) == 0 && request.Term >= rf.currentTerm {
		rf.currentTerm = request.Term
		DPrintf(dClient, "S%v recv heartbeat from S%v", rf.me, request.LeaderId)
		response.Term, response.Success = rf.currentTerm, true
	}
	//2. Reply false if log doesn’t contain an entry at prevLogIndex
	//whose term matches prevLogTerm (§5.3)
	if request.PrevLogIndex != rf.GetPrevLogIndex() || request.PrevLogIndex == rf.GetPrevLogIndex() && request.PrevLogTerm != rf.GetPrevLogTerm() {
		response.Term, response.Success = rf.currentTerm, false
		return
	}
	//3. If an existing entry conflicts with a new one (same index but different terms),
	//   delete the existing entry and all that follow it (§5.3)
	if rf.EntryConflict(request) {
		for index, entry := range request.Entries {
			if entry.term != rf.Entries[index].term {
				rf.Entries = rf.Entries[:index]
				rf.Entries = append(rf.Entries, request.Entries[index:]...)
				response.MatchIndex = len(rf.Entries) - 1
				break
			}
		}
	}
	//4. Append any new entries not already in the log
	if rf.currentTerm == request.Term && request.PrevLogIndex == rf.GetPrevLogIndex() && request.PrevLogTerm == rf.GetPrevLogTerm() {
		rf.Entries = append(rf.Entries, request.Entries...)
		response.MatchIndex = len(rf.Entries) - 1
	}
	//5. If leaderCommit > commitIndex, set commitIndex
	//min(leaderCommit, index of last new entry)
	if request.LeaderCommitIndex > rf.commitIndex {
		rf.commitIndex = min(request.LeaderCommitIndex, len(rf.Entries)-1)

	}
}
func (rf *Raft) EntryConflict(request *AppendEntriesArgs) bool {
	if len(rf.Entries) > request.PrevLogIndex+1 || len(rf.Entries) == request.PrevLogIndex && rf.Entries[request.PrevLogIndex].term != request.Entries[0].term {
		return false
	}
	return true
}

//// PrevLogMatch 检查PrevLogIndex和PrevLogTerm是否匹配
//func (rf *Raft) PrevLogMatch(request *AppendEntriesArgs, response *AppendEntriesReply) bool {
//	if len(rf.Entries) <= request.PrevLogIndex || rf.Entries[request.PrevLogIndex].term != request.PrevLogTerm {
//		response.Term, response.Success = rf.currentTerm, false
//		return false
//	}
//	return true
//}
//
//// EntryConflict 检查日志条目是否冲突
//func (rf *Raft) EntryConflict(request *AppendEntriesArgs, response *AppendEntriesReply) bool {
//	if len(rf.Entries) > request.PrevLogIndex+1 && rf.Entries[request.PrevLogIndex+1].term != request.Entries[0].term {
//		rf.Entries = rf.Entries[:request.PrevLogIndex+1]
//	}
//	return false
//}

// StartElection 在Election timeout之后发起选举
func (rf *Raft) StartElection() {
	DPrintf(dTimer, "S%v start election", rf.me)
	rf.mu.Lock()
	rf.votedFor = rf.me
	defer rf.mu.Unlock()
	var grantedVotes = 1
	requestVoteArgs := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.Entries) - 1, //LastLogTerm:  rf.Entries[len(rf.Entries)-1].term,
	}
	if len(rf.Entries) == 0 {
		requestVoteArgs.LastLogTerm = -1
	} else {
		requestVoteArgs.LastLogTerm = rf.Entries[len(rf.Entries)-1].term
	}
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			reply := &RequestVoteReply{}
			if rf.sendRequestVote(peer, requestVoteArgs, reply) {
				if rf.currentTerm == requestVoteArgs.Term && rf.state == "candidate" {
					if reply.VotedGranted {
						grantedVotes += 1
						if grantedVotes > len(rf.peers)/2 {
							rf.state = "leader"
							DPrintf(dLeader, "S%v become leader term %v", rf.me, rf.currentTerm)
							return
							//rf.BroadcastHeartBeat()
						}
					}
				} else if reply.Term > rf.currentTerm {
					DPrintf(dClient, "S%v finds new leader S%v with Term %v", rf.me, peer, reply.Term)
					rf.currentTerm = reply.Term
					rf.state = "follower"
					rf.votedFor = -1
				}
			}
		}(peer)

	}
}

func (rf *Raft) GetPrevLogIndex() int {
	return len(rf.Entries) - 1
}

func (rf *Raft) GetPrevLogTerm() int {
	if len(rf.Entries) == 0 {
		return -1
	}
	return rf.Entries[len(rf.Entries)-1].term
}

// RandomElectionDuration 随机初始化选举超时时间
func RandomElectionDuration() time.Duration {
	return minElectionTimeout*time.Millisecond + time.Millisecond*time.Duration(rand.Int63n(maxElectionTimeout-minElectionTimeout))
}

// Make the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:          peers,
		persister:      persister,
		me:             me,
		dead:           0,
		currentTerm:    0,
		votedFor:       -1,
		Entries:        make([]LogEntry, 1),
		commitIndex:    -1,
		state:          "follower",
		lastApplied:    -1,
		nextIndex:      make([]int, len(peers)),
		matchIndex:     make([]int, len(peers)),
		heartbeatTimer: time.NewTimer(heartbeatTimeout * time.Millisecond),
		electionTimer:  time.NewTimer(RandomElectionDuration()),
	}
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	DPrintf(dLog, "S%v start up", rf.me)
	go rf.ticker()
	return rf
}
