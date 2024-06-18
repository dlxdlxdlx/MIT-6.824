package raft

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

type AppendEntriesArgs struct {
	Term            int        //leader's Term
	LeaderId        int        //leader id
	PrevLogIndex    int        // index of log entry immediately precceding new ones
	PrevLogTerm     int        // Term of PrevLogIndex entry
	Entries         []LogEntry //log Entries to store(empty for heartBeat; may send more than one for efficiency)
	LeaderCommitIdx int        //leader's commitIndex
}
type AppendEntriesReply struct {
	Term         int //currentTerm, for leader to update itself
	ConflictIdx  int
	ConflictTerm int
	Success      bool //true if follower contained entry matching PrevLogIndex and PrevLogTerm
}

// ApplyMsg as each Raft clientEnd becomes aware that successive log Entries are
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
	CommandTerm  int
	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}
type LogEntry struct {
	Term    int
	Index   int
	Payload interface{}
}

func (rf *Raft) sendAppendEntries(peer int, args *AppendEntriesArgs, replys *AppendEntriesReply) bool {
	ok := rf.peers[peer].Call("Raft.AppendEntries", args, replys)
	return ok
}
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
