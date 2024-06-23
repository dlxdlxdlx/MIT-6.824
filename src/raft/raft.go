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
	"6.824/labgob"
	"bytes"
	"math/rand"
	"sort"

	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

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
	currentTerm    int
	votedFor       int
	Entries        []LogEntry

	commitIndex int
	lastApplied int // 针对appCh 提交
	nextIndex   []int
	matchIndex  []int

	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
}

func (rf *Raft) BroadcastHeartbeat(isHeartBeat bool) {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		if isHeartBeat {
			go rf.replicateOneRound(peer)
		} else {
			rf.replicatorCond[peer].Signal()
		}
	}
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	rf.persister.SaveRaftState(rf.EncodeState())
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var Entries []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&Entries) != nil {
		DPrintf(dError, "S%v readPersist error", rf.me)
	}
	rf.currentTerm, rf.votedFor, rf.Entries = currentTerm, voteFor, Entries

	rf.lastApplied, rf.commitIndex = rf.Entries[0].Index, rf.Entries[0].Index
}

func (rf *Raft) EncodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.Entries)
	return w.Bytes()
}

// CondInstallSnapshot A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if lastIncludedIndex <= rf.commitIndex {
		return false
	}

	if lastIncludedIndex > rf.getLastLog().Index {
		rf.Entries = make([]LogEntry, 1)
	} else {
		rf.shrinkEntries(rf.Entries[lastIncludedIndex-rf.getFirstLog().Index:])
		rf.Entries[0].Payload = nil
	}
	rf.Entries[0].Index, rf.Entries[0].Term = lastIncludedIndex, lastIncludedTerm
	rf.commitIndex, rf.lastApplied = lastIncludedIndex, lastIncludedIndex

	rf.persister.SaveStateAndSnapshot(rf.EncodeState(), snapshot)
	return true
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()

	defer rf.mu.Unlock()
	snapshotIdx := rf.getFirstLog().Index
	if index <= snapshotIdx {
		return
	}
	rf.shrinkEntries(rf.Entries[index-snapshotIdx:])
	rf.Entries[0].Payload = nil
	rf.persister.SaveStateAndSnapshot(rf.EncodeState(), snapshot)

}

func (rf *Raft) InstallSnapshot(request *InstallSnapshotRequest, response *InstallSnapshotResponse) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	response.Term = rf.currentTerm
	if request.Term < rf.currentTerm {
		return
	}
	if request.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = request.Term, -1
		rf.persist()
	}
	rf.changeState(Follower)
	rf.electionTimer.Reset(RandomElectionDuration())

	if request.LastIncludedIndex <= rf.commitIndex {
		return
	}

	go func() {
		rf.appCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      request.Data,
			SnapshotIndex: request.LastIncludedIndex,
			SnapshotTerm:  request.LastIncludedTerm,
		}
	}()
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(request *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if request.Term < rf.currentTerm || (request.Term == rf.currentTerm && rf.votedFor != -1) {
		reply.Term, reply.VotedGranted = rf.currentTerm, false
		return
	}
	if request.Term > rf.currentTerm {
		rf.changeState(Follower)
		rf.currentTerm, rf.votedFor = request.Term, -1
	}
	isLogUpdateToDate := rf.LogUpToDate(request.LastLogTerm, request.LastLogIndex)
	if !isLogUpdateToDate {
		reply.Term, reply.VotedGranted = rf.currentTerm, false
		return
	}
	DPrintf(dLog, "S%v recvd vote request from S%v self-state:{CT:%v VF: %v lastLog:%v}  requestVoteArgs:%v , isLogUpdateTodate:%v", rf.me, request.CandidateId, rf.currentTerm, rf.votedFor, rf.getLastLog(), request, isLogUpdateToDate)
	rf.electionTimer.Reset(RandomElectionDuration())
	rf.votedFor = request.CandidateId
	reply.Term, reply.VotedGranted = rf.currentTerm, true
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return -1, -1, false
	}
	// Your code here (2B).
	entry := rf.appendNewEntry(command)
	DPrintf(dLeader, "S%v deliver entry:%v", rf.me, entry)
	rf.BroadcastHeartbeat(false)
	return entry.Index, entry.Term, rf.state == Leader
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
	//DPrintf(dWarn, "S%v killed", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this clientEnd hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			rf.StartElection()
			rf.electionTimer.Reset(RandomElectionDuration())
			rf.mu.Unlock()
		case <-rf.heartbeatTimer.C:
			_, isLeader := rf.GetState()
			rf.mu.Lock()
			if isLeader {
				rf.BroadcastHeartbeat(true)
				rf.heartbeatTimer.Reset(heartbeatTimeout * time.Millisecond)
			}
			rf.mu.Unlock()
		}

	}
}

// AppendEntries 添加日志条目, 同时也可以作为心跳机制RPC调用
func (rf *Raft) AppendEntries(request *AppendEntriesArgs, response *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if request.Term < rf.currentTerm {
		response.Term, response.Success = rf.currentTerm, false
		return
	}
	if request.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = request.Term, -1
	}
	rf.changeState(Follower)
	rf.electionTimer.Reset(RandomElectionDuration())
	if request.PrevLogIndex < rf.getFirstLog().Index {
		response.Term, response.Success = 0, false
		DPrintf(dLog, "S%v recv unknown RPC from %v", rf.me, request.LeaderId)
		return
	}
	if !rf.matchLog(request.PrevLogTerm, request.PrevLogIndex) {
		response.Term, response.Success = rf.currentTerm, false
		lastIdx := rf.getLastLog().Index
		if lastIdx < request.PrevLogIndex {
			response.ConflictTerm, response.ConflictIdx = -1, lastIdx+1
		} else {
			firstIdx := rf.getFirstLog().Index
			response.ConflictTerm = rf.Entries[request.PrevLogIndex-firstIdx].Term
			idx := request.PrevLogIndex - 1
			for idx >= firstIdx && rf.Entries[idx-firstIdx].Term == response.ConflictTerm {
				idx--
			}
			response.ConflictIdx = idx
		}
		return
	}
	DPrintf(dLog, "S%v recv AE from S%v currentstate:{CT:%v LastLogIndex:%v LostLogTerm:%v}  requestArgs:{T:%v PrevLogIndex:%v PrevLogTerm:%v }", rf.me, request.LeaderId, rf.currentTerm, rf.getLastLog().Index, rf.getLastLog().Term, request.Term, request.PrevLogIndex, request.PrevLogTerm)
	firstIdx := rf.getFirstLog().Index
	for idx, entry := range request.Entries {
		if entry.Index-firstIdx >= len(rf.Entries) || rf.Entries[entry.Index-firstIdx].Term != entry.Term {
			rf.shrinkEntries(append(rf.Entries[:entry.Index-firstIdx], request.Entries[idx:]...))
			break
		}
	}
	rf.updateCommitIdxForFollower(request.LeaderCommitIdx)

	response.Term, response.Success = rf.currentTerm, true
	return
}

// StartElection 在Election timeout之后发起选举
func (rf *Raft) StartElection() {
	rf.changeState(Candidate)
	rf.votedFor = rf.me
	rf.currentTerm += 1
	var grantedVotes = 1
	requestVoteArgs := rf.createRequestVoteArgs()
	rf.persist()
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			reply := &RequestVoteReply{}
			if rf.sendRequestVote(peer, requestVoteArgs, reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.currentTerm == requestVoteArgs.Term && rf.state == Candidate {
					if reply.VotedGranted {
						grantedVotes += 1
						if grantedVotes > len(rf.peers)/2 {
							rf.changeState(Leader)
							DPrintf(dInfo, "S%v become leader Term %v last data:%v", rf.me, rf.currentTerm, rf.Entries[len(rf.Entries)-1].String())
							rf.BroadcastHeartbeat(true)
						}
					} else if reply.Term > rf.currentTerm {
						rf.changeState(Follower)
						rf.currentTerm, rf.votedFor = reply.Term, -1
						rf.persist()
					}
				}
			}
		}(peer)

	}
}

func (rf *Raft) needReplicate(peer int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state == Leader && rf.matchIndex[peer] < rf.getLastLog().Index
}

func (rf *Raft) replicateOneRound(peer int) {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	prevLogIdx := rf.nextIndex[peer] - 1
	if prevLogIdx < rf.getFirstLog().Index {
		//todo
		firstLog := rf.getFirstLog()
		request := &InstallSnapshotRequest{
			Term:              rf.currentTerm,
			LeaderId:          rf.me,
			LastIncludedIndex: firstLog.Index,
			LastIncludedTerm:  firstLog.Term,
			Data:              rf.persister.ReadSnapshot(),
		}
		rf.mu.Unlock()
		response := &InstallSnapshotResponse{}
		if rf.sendInstallSnapshot(peer, request, response) {
			rf.mu.Lock()
			rf.handleInstallSnapshotResponse(peer, request, response)
			rf.mu.Unlock()

		}
	} else {
		request := rf.createAppendEntriesRequest(prevLogIdx)
		rf.mu.Unlock()
		response := new(AppendEntriesReply)
		if rf.sendAppendEntries(peer, request, response) {
			rf.mu.Lock()
			rf.handleAppendEntriesResponse(peer, request, response)
			rf.mu.Unlock()
		}
	}
}
func (rf *Raft) handleInstallSnapshotResponse(peer int, request *InstallSnapshotRequest, response *InstallSnapshotResponse) {
	if rf.state == Leader && rf.currentTerm == request.Term {
		if response.Term > rf.currentTerm {
			rf.changeState(Follower)
			rf.currentTerm, rf.votedFor = response.Term, -1
			rf.persist()
		}
	} else {
		rf.matchIndex[peer], rf.nextIndex[peer] = request.LastIncludedIndex, request.LastIncludedIndex+1
	}
}
func (rf *Raft) sendInstallSnapshot(server int, request *InstallSnapshotRequest, response *InstallSnapshotResponse) bool {
	return rf.peers[server].Call("Raft.InstallSnapshot", request, response)
}
func (rf *Raft) replicator(peer int) {
	rf.replicatorCond[peer].L.Lock()
	defer rf.replicatorCond[peer].L.Unlock()
	for !rf.killed() {
		for !rf.needReplicate(peer) {
			rf.replicatorCond[peer].Wait()
		}
		rf.replicateOneRound(peer)
	}
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
		votedFor:       -1,
		Entries:        make([]LogEntry, 1),
		appCh:          applyCh,
		state:          Follower,
		nextIndex:      make([]int, len(peers)),
		matchIndex:     make([]int, len(peers)),
		replicatorCond: make([]*sync.Cond, len(peers)),
		heartbeatTimer: time.NewTimer(heartbeatTimeout * time.Millisecond),
		electionTimer:  time.NewTimer(RandomElectionDuration()),
	}
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.applyCond = sync.NewCond(&rf.mu)
	lastLog := rf.getFirstLog()
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i], rf.matchIndex[i] = lastLog.Index+1, 0
		if i != rf.me {
			rf.replicatorCond[i] = sync.NewCond(&sync.Mutex{})
			go rf.replicator(i)
		}
	}
	// start ticker goroutine to start elections
	DPrintf(dLog, "S%v start up", rf.me)
	go rf.ticker()
	go rf.applier()
	return rf
}

// applier 负责向appCh传输已提交的日志条目,使用lastApplied和commitIndex评估哪些日志需要被提交
func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		firstIdx, commitIdx, lastApplied := rf.getFirstLog().Index, rf.commitIndex, rf.lastApplied
		entries := make([]LogEntry, commitIdx-lastApplied)
		copy(entries, rf.Entries[lastApplied+1-firstIdx:commitIdx+1-firstIdx])
		DPrintf(dClient, "S%v start apply msgs Index %v ~ %v in term %v", rf.me, rf.lastApplied, rf.commitIndex, rf.currentTerm)
		for _, entry := range entries {
			rf.appCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Payload,
				CommandIndex: entry.Index,
				CommandTerm:  entry.Term,
			}
			DPrintf(dClient, "S%v entry:%v applied", rf.me, entry)
		}
		rf.lastApplied = max(rf.lastApplied, rf.commitIndex)
		rf.mu.Unlock()
	}
}

// --- utils ---

// getLastLogIndex 获取当前节点最近的Log索引
func (rf *Raft) getLastLog() LogEntry {
	return rf.Entries[len(rf.Entries)-1]
}
func (rf *Raft) getFirstLog() LogEntry {
	return rf.Entries[0]
}
func (rf *Raft) matchLog(term, index int) bool {
	return index <= rf.getLastLog().Index && term == rf.Entries[index-rf.getFirstLog().Index].Term
}
func (rf *Raft) appendNewEntry(command interface{}) LogEntry {
	lastLog := rf.getLastLog()
	newLog := LogEntry{
		Term:    rf.currentTerm,
		Payload: command,
		Index:   lastLog.Index + 1,
	}
	rf.Entries = append(rf.Entries, newLog)
	rf.matchIndex[rf.me], rf.nextIndex[rf.me] = newLog.Index, newLog.Index+1
	rf.persist()
	return newLog
}

// changeState 改变角色状态
func (rf *Raft) changeState(role Role) {
	if rf.state == role {
		return
	}
	DPrintf(dLog2, "S%v changed from %v -> %v", rf.me, rf.state, role)
	rf.state = role
	switch rf.state {
	case Follower:
		rf.heartbeatTimer.Stop()
		rf.electionTimer.Reset(RandomElectionDuration())
	case Leader:
		lastLog := rf.getLastLog()
		for i := 0; i < len(rf.peers); i++ {
			rf.matchIndex[i], rf.nextIndex[i] = 0, lastLog.Index+1
		}
		rf.electionTimer.Stop()
		rf.heartbeatTimer.Reset(heartbeatTimeout * time.Millisecond)
	case Candidate:
	default:
		panic("unhandled default case")
	}
}

// createAppendEntriesRequest 创建AppendEntries请求
func (rf *Raft) createAppendEntriesRequest(prevLogIndex int) *AppendEntriesArgs {
	firstIdx := rf.getFirstLog().Index
	//slice数组是左开右闭
	entries := make([]LogEntry, len(rf.Entries[prevLogIndex+1-firstIdx:]))
	copy(entries, rf.Entries[prevLogIndex+1-firstIdx:])
	return &AppendEntriesArgs{
		Term:            rf.currentTerm,
		LeaderId:        rf.me,
		PrevLogIndex:    prevLogIndex,
		PrevLogTerm:     rf.Entries[prevLogIndex-firstIdx].Term, //Entries:         entries,
		Entries:         entries,
		LeaderCommitIdx: rf.commitIndex,
	}
}

func (rf *Raft) handleAppendEntriesResponse(peer int, request *AppendEntriesArgs, response *AppendEntriesReply) {
	if rf.state == Leader && rf.currentTerm == request.Term {
		if response.Success {
			rf.matchIndex[peer] = request.PrevLogIndex + len(request.Entries)
			rf.nextIndex[peer] = rf.matchIndex[peer] + 1
			//DPrintf(dLeader, "S%v S%v append succeed nextIndex State:%v matchIndex state:%v", rf.me, peer, rf.nextIndex, rf.matchIndex)
			rf.updateCommitIdxForLeader()
		} else {
			if response.Term > rf.currentTerm {
				rf.changeState(Follower)
				rf.currentTerm, rf.votedFor = response.Term, -1
				rf.persist()
			} else if response.Term == rf.currentTerm {
				rf.nextIndex[peer] = response.ConflictIdx
				if response.ConflictTerm != -1 {
					firstIndex := rf.getFirstLog().Index
					for i := request.PrevLogIndex; i >= firstIndex; i-- {
						if rf.Entries[i-firstIndex].Term == response.ConflictTerm {
							rf.nextIndex[peer] = i + 1
							break
						}
					}
				}
			}

		}
	}
}

func (rf *Raft) updateCommitIdxForLeader() {
	n := len(rf.matchIndex)
	dist := make([]int, n)
	copy(dist, rf.matchIndex)
	sort.Ints(dist)
	newCommitIdx := dist[n-(n/2+1)]
	if newCommitIdx > rf.commitIndex {
		if rf.matchLog(rf.currentTerm, newCommitIdx) {
			DPrintf(dLeader, "S%v update commitIdx from %v->%v", rf.me, rf.commitIndex, newCommitIdx)
			rf.commitIndex = newCommitIdx
			rf.applyCond.Signal()
		}
	}
}
func (rf *Raft) updateCommitIdxForFollower(commitIdx int) {
	if rf.state != Follower {
		return
	}
	newCommitIdx := min(commitIdx, rf.getLastLog().Index)
	if newCommitIdx > rf.commitIndex {
		DPrintf(dLog2, "S%v update commit from %v -> %v in term %v", rf.me, rf.commitIndex, newCommitIdx, rf.currentTerm)
		rf.commitIndex = newCommitIdx
		rf.applyCond.Signal()
	}
}

func (rf *Raft) shrinkEntries(entries []LogEntry) {
	const lenMultiple = 2
	if len(entries)*lenMultiple < cap(entries) {
		newEntries := make([]LogEntry, len(entries))
		copy(newEntries, entries)
		rf.Entries = newEntries
		return
	}
	rf.Entries = entries
}

func (rf *Raft) LogUpToDate(logTerm, logIndex int) bool {
	lastLog := rf.getLastLog()
	return logTerm > lastLog.Term || (logTerm == lastLog.Term && logIndex >= lastLog.Index)
}
