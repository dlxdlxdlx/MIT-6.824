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
//   each time a new entry is committed to the log, each Raft clientEnd
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

// as each Raft clientEnd becomes aware that successive log Entries are
// committed, the clientEnd should send an ApplyMsg to the service (or
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
	Term    int
	Payload interface{}
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
	minElectionTimeout = 250
	maxElectionTimeout = 350
)

type Role string
type peerState struct {
	clientEnd  *labrpc.ClientEnd
	nextIndex  int
	matchIndex int
}

// Raft A Go object implementing a single Raft clientEnd.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this clientEnd's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this clientEnd's persisted state
	me        int                 // this clientEnd's index into peers[]
	dead      int32               // set by Kill()

	// Your Payload here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	appCh          chan ApplyMsg
	applyCond      *sync.Cond
	replicatorCond []*sync.Cond
	state          Role
	//peers          []peerState
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
	//rf.mu.Lock()
	//DPrintf(dLog2, "S%v broadCast get lock", rf.me)

	defer rf.heartbeatTimer.Reset(heartbeatTimeout * time.Millisecond)
	defer rf.electionTimer.Reset(RandomElectionDuration())
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
		request.PrevLogTerm = rf.Entries[request.PrevLogTerm].Term
		request.PrevLogIndex = len(rf.Entries) - 1
	}
	//defer rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		response := &AppendEntriesReply{}
		go func(peer int) {
			rf.peers[peer].Call("Raft.AppendEntries", request, response)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if !response.Success {
				if response.Term > rf.currentTerm {
					rf.state = "follower"
					rf.currentTerm = response.Term
					return
				}
			}

		}(i)
	}
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	currentTerm, state := rf.currentTerm, rf.state == "leader"
	return currentTerm, state
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
	// Payload := w.Bytes()
	// rf.persister.SaveRaftState(Payload)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(Payload)
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
	// Your Payload here (2A, 2B).
	Term         int //candidate's Term
	CandidateId  int // candidate requesting vote
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your Payload here (2A).
	Term         int
	VotedGranted bool
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(request *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	DPrintf(dLog2, "S%v request_vote get lock", rf.me)
	rf.electionTimer.Reset(RandomElectionDuration())
	defer rf.mu.Unlock()
	DPrintf(dLog, "S%v recvd vote request from S%v self-state:{CT:%v VF: %v} candidate-state:{CT:%v}", rf.me, request.CandidateId, rf.currentTerm, rf.votedFor, request.Term)
	if request.Term < rf.currentTerm || (request.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != request.CandidateId) {
		reply.Term, reply.VotedGranted = rf.currentTerm, false
		return
	}
	if request.Term == rf.currentTerm && rf.votedFor != -1 {
		reply.Term, reply.VotedGranted = rf.currentTerm, false
		return
	}
	if request.Term > rf.currentTerm {
		rf.state = "follower"
		rf.currentTerm, rf.votedFor = request.Term, request.CandidateId
		DPrintf(dLog2, "S%v vote for S%v Term: %v", rf.me, request.CandidateId, rf.currentTerm)
		reply.Term, reply.VotedGranted = rf.currentTerm, true
		return
	}
	if rf.Entries[len(rf.Entries)-1].Term > request.LastLogTerm || (rf.Entries[len(rf.Entries)-1].Term == request.LastLogTerm && len(rf.Entries) > request.LastLogIndex) {
		reply.Term, reply.VotedGranted = rf.currentTerm, false
		return
	}
	DPrintf(dClient, "S%v vote for S%v Term:%v", rf.me, request.CandidateId, rf.currentTerm)
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

func (rf *Raft) appendNewEntry(command interface{}) LogEntry {

	entry := LogEntry{
		Term:    rf.currentTerm,
		Payload: command,
	}
	rf.Entries = append(rf.Entries, entry)
	rf.lastApplied++
	return entry
}
func (rf *Raft) replicator() {
	DPrintf(dLeader, "S%v start replicate", rf.me)
	appendEntryArgs := &AppendEntriesArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		PrevLogIndex:      rf.lastApplied - 1, //PrevLogTerm:       rf.Entries[rf.lastApplied].Term,
		LeaderCommitIndex: rf.commitIndex,
	}
	//appendEntryArgs.Entries = append(rf.Entries)
	if rf.lastApplied == 0 {
		appendEntryArgs.PrevLogIndex = -1
	} else {
		appendEntryArgs.PrevLogTerm = rf.Entries[rf.lastApplied-1].Term
	}
	for i, peer := range rf.peers {
		appendEntryArgs.Entries = rf.Entries[rf.nextIndex[i]:]
		if i == rf.me {
			continue
		}
		go func(index int, peer *labrpc.ClientEnd) {
			appendEntryReply := &AppendEntriesReply{}
			// 针对每一个peer 发送AppendEntries RPC
			// 1.直接能append成功
			// 2. append失败开始进行shrink操作
			//appendEntryArgs.Entries =
			//TODO
			ok := peer.Call("Raft.AppendEntries", appendEntryArgs, appendEntryReply)
			for !ok || !appendEntryReply.Success {
				// 找到能够匹配的最大Index
				if !appendEntryReply.Success {
					DPrintf(dLog, "S%v append entries failed", rf.me)
					if rf.nextIndex[index] > 0 {
						rf.nextIndex[index]--
					}
					appendEntryArgs.Entries = rf.Entries[rf.nextIndex[index]:]
					ok = peer.Call("Raft.AppendEntries", appendEntryArgs, appendEntryReply)
				}
			}
			rf.nextIndex[index] = appendEntryReply.MatchIndex + 1
			rf.matchIndex[index] = appendEntryReply.MatchIndex

		}(i, peer)
	}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != "leader" {
		return index, term, false
	}
	// Your code here (2B).
	rf.appendNewEntry(command)
	DPrintf(dLeader, "S%v save log %v", rf.me, command)
	index = rf.lastApplied
	term = rf.currentTerm
	rf.replicator()
	rf.updateCommitIndex()
	return index, term, rf.state == "leader"
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
	DPrintf(dWarn, "S%v killed", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this clientEnd hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select {
		case <-rf.electionTimer.C:
			rf.StartElection()
		case <-rf.heartbeatTimer.C:
			_, isLeader := rf.GetState()
			if isLeader {
				rf.mu.Lock()
				rf.BroadcastHeartBeat()
				rf.mu.Unlock()
			}
		}

	}
}

func (rf *Raft) updateCommitIndex() {
	//DPrintf(dLog, "S%v update commit index", rf.me)
	for i := len(rf.Entries) - 1; i > rf.commitIndex; i-- {
		count := 1
		for j := 0; j < len(rf.peers); j++ {
			if rf.matchIndex[j] >= i {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = i
			break
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.electionTimer.Reset(RandomElectionDuration())
	//1. Reply false if Term < currentTerm (§5.1)
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	if request.Term < rf.currentTerm {
		response.Term, response.Success = rf.currentTerm, false
		return
	}
	if request.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = request.Term, -1
	}
	if rf.state != "follower" && rf.currentTerm <= request.Term {
		//选举还没有完成就收到心跳包的话就自动变成follower
		DPrintf(dClient, "S%v recv RPC from higher term state: %v become follower", rf.me, rf.state)
		rf.state = "follower"
		rf.currentTerm = request.Term
		return
	}
	// 如果是心跳包的话
	//DPrintf(dLog2, "S%v clientEnd:{term:%v lastApplied:%v} leader:{term:%v prevLogIndex:%v prevLogTerm:%v} isHeartBeat:%v", rf.me, rf.currentTerm, rf.lastApplied, request.Term, request.PrevLogIndex, request.PrevLogTerm, len(request.Entries) == 0)
	if len(request.Entries) == 0 && request.Term >= rf.currentTerm {
		rf.currentTerm = request.Term
		//DPrintf(dClient, "S%v recv heartbeat from S%v", rf.me, request.LeaderId)
		response.Term, response.Success = rf.currentTerm, true
		return
	}
	DPrintf(dLog2, "S%v recv append entry from S%v, leader state:{prevLogTerm:%v PrevLogIndex:%v}  clientEnd state:{lastApplied: %v}", rf.me, request.LeaderId, request.PrevLogTerm, request.PrevLogIndex, rf.lastApplied)
	// 评估日志冲突以及解决日志冲突
	//2. Reply false if log doesn’t contain an entry at prevLogIndex
	//whose Term matches prevLogTerm (§5.3)
	//if request.PrevLogIndex != rf.lastApplied || (rf.lastApplied != -1 && request.PrevLogIndex == rf.GetPrevLogIndex() && request.PrevLogTerm != rf.GetPrevLogTerm()) {
	//	response.Term, response.Success = rf.currentTerm, false
	//	return
	//}
	//DPrintf(dLog, "S%v append entry:%v entryLen:%v", rf.me, request.Entries[0].Payload, len(rf.Entries))
	//3. If an existing entry conflicts with a new one (same index but different terms),
	//   delete the existing entry and all that follow it (§5.3)
	if rf.EntryConflict(request) {
		DPrintf(dLog2, "S%v conflict occur", rf.me)
		response.Success = false
		response.Term = rf.currentTerm
		response.MatchIndex = rf.lastApplied - 1
	} else {
		DPrintf(dLog2, "S%v append entry success", rf.me)
		response.Term, response.Success = rf.currentTerm, true
		rf.Entries = append(rf.Entries, request.Entries...)
		rf.lastApplied += len(request.Entries)
		response.MatchIndex = rf.lastApplied
		return
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
	if request.PrevLogIndex != rf.lastApplied || (rf.lastApplied != -1 && request.PrevLogIndex == rf.GetPrevLogIndex() && request.PrevLogTerm != rf.GetPrevLogTerm()) {
		return true
	}
	return false
}

// StartElection 在Election timeout之后发起选举
func (rf *Raft) StartElection() {
	rf.mu.Lock()
	DPrintf(dTimer, "S%v start election", rf.me)
	//DPrintf(dLog2, "S%v start_election getLock", rf.me)
	rf.electionTimer.Reset(RandomElectionDuration())
	rf.votedFor = rf.me
	rf.currentTerm += 1
	rf.state = "candidate"
	var grantedVotes = 1
	requestVoteArgs := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.lastApplied, //LastLogTerm:  rf.Entries[len(rf.Entries)-1].Term,
	}
	if rf.lastApplied == -1 {
		requestVoteArgs.LastLogTerm = -1
	} else {
		requestVoteArgs.LastLogTerm = rf.Entries[len(rf.Entries)-1].Term
	}
	rf.mu.Unlock()
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			reply := &RequestVoteReply{}
			if rf.sendRequestVote(peer, requestVoteArgs, reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.currentTerm == requestVoteArgs.Term && rf.state == "candidate" {
					if reply.VotedGranted {
						grantedVotes += 1
						if grantedVotes > len(rf.peers)/2 {
							rf.state = "leader"
							DPrintf(dInfo, "S%v become leader Term %v", rf.me, rf.currentTerm)
							rf.BroadcastHeartBeat()
							return
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
	return rf.Entries[len(rf.Entries)-1].Term
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
