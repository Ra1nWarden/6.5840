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
	//	"bytes"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

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

// Role Raft instances: Follower / Candidate / Leader
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

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

	// 3A
	// Persisted state on all servers
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile state on all servers
	commitIndex       int
	lastApplied       int
	role              Role
	electionTimeOut   time.Duration
	lastPing          time.Time
	lastElectionStart time.Time
	leaderId          int

	// Volatile state on leaders
}

type LogEntry struct {
	term int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (3A).
	rf.mu.Lock()
	term := rf.currentTerm
	isleader := rf.role == Leader
	rf.mu.Unlock()
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

// Helper for get last log term
func (rf *Raft) getLastLogTerm() int {
	if len(rf.log) == 0 {
		return 0
	}
	return rf.log[len(rf.log)-1].term
}

func (rf *Raft) fallbackToFollowerWithNewTerm(newTerm int) {
	rf.mu.Lock()
	rf.currentTerm = newTerm
	rf.votedFor = -1
	rf.transitRole(Follower)
	rf.mu.Unlock()
}

// Transition between roles - needs lock
func (rf *Raft) transitRole(role Role) {
	rf.role = role
	switch role {
	case Follower:
		rf.leaderId = -1
	case Candidate:
		rf.currentTerm++
		rf.votedFor = rf.me
	case Leader:
		rf.leaderId = rf.me
	}
}

// Start election
func (rf *Raft) startElectionWithArgs(args *RequestVoteArgs) {
	votesChan := make(chan bool)
	for server := 0; server < len(rf.peers); server++ {
		if server == rf.me {
			continue
		}
		DPrintf("server %d sent request vote to server %d\n", rf.me, server)
		go rf.sendRequestVoteAndHandle(server, args, votesChan)
	}
	votes := 1
	for server := 0; server < len(rf.peers)-1; server++ {
		if <-votesChan {
			votes++
		}
	}
	if votes > len(rf.peers)/2 {
		DPrintf("%d won election term %d\n", args.CandidateId, args.Term)
		rf.mu.Lock()
		if rf.role == Candidate && rf.currentTerm == args.Term {
			rf.transitRole(Leader)
			args := &AppendEntriesArgs{
				Term:     rf.currentTerm,
				LeaderId: rf.me,
			}
			rf.mu.Unlock()
			rf.sendHeartBeatsWithArgs(args)
		} else {
			rf.mu.Unlock()
		}
	}
}

// As leader, send heartbeats to all peers
func (rf *Raft) sendHeartBeatsWithArgs(args *AppendEntriesArgs) {
	for server := 0; server < len(rf.peers); server++ {
		if server == args.LeaderId {
			continue
		}
		DPrintf("leader %d sends heart beat to %d with term %d\n", args.LeaderId, server, args.Term)
		go rf.sendHeartBeatsAndHandle(server, args)
	}
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
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) sendRequestVoteAndHandle(server int, args *RequestVoteArgs, result chan<- bool) {
	reply := &RequestVoteReply{}
	if rf.peers[server].Call("Raft.RequestVote", args, reply) {
		DPrintf("server %d vote %t for %d for term %d\n", server, reply.VoteGranted, args.CandidateId, args.Term)
		if reply.VoteGranted {
			result <- true
		} else if reply.Term > args.Term {
			rf.fallbackToFollowerWithNewTerm(reply.Term)
		}
	}
	result <- false
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	DPrintf("server %d handles vote from %d for term %d\n", rf.me, args.CandidateId, args.Term)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.mu.Unlock()
		return
	}

	if args.Term == rf.currentTerm && rf.votedFor != -1 {
		reply.Term = rf.currentTerm
		reply.VoteGranted = (rf.votedFor == args.CandidateId)
		DPrintf("server %d already voted for %d in term %d\n", rf.me, rf.votedFor, rf.currentTerm)
		rf.mu.Unlock()
		return
	}

	// Update current term and reset vote
	if args.Term > rf.currentTerm {
		DPrintf("server %d transit to follower after receiving a higher term %d from %d\n", rf.me, args.Term, args.CandidateId)
		rf.transitRole(Follower)
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	// Check if candidate's log is at least as up-to-date as the current instance
	willVote := false
	if args.LastLogTerm == rf.getLastLogTerm() {
		willVote = args.LastLogIndex >= len(rf.log)
	} else {
		willVote = args.LastLogTerm > rf.getLastLogTerm()
	}

	// Update vote
	if willVote {
		rf.votedFor = args.CandidateId
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = willVote
	rf.mu.Unlock()
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

// AppendEntries RPC

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		DPrintf("server %d rejects heart beat from %d with higher term %d\n", rf.me, args.LeaderId, rf.currentTerm)
		rf.mu.Unlock()
		return
	}

	shouldTransitToFollower := false
	if rf.role == Candidate {
		shouldTransitToFollower = args.Term >= rf.currentTerm
	} else if rf.role == Leader {
		shouldTransitToFollower = args.Term > rf.currentTerm
	}

	if shouldTransitToFollower {
		DPrintf("server %d transit back to follower\n", rf.me)
		rf.transitRole(Follower)
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	rf.lastPing = time.Now()
	rf.leaderId = args.LeaderId
	reply.Success = true
	reply.Term = rf.currentTerm
	rf.mu.Unlock()
}

func (rf *Raft) sendHeartBeatsAndHandle(server int, args *AppendEntriesArgs) {
	reply := &AppendEntriesReply{}
	if rf.peers[server].Call("Raft.AppendEntries", args, reply) && !reply.Success && reply.Term > args.Term {
		rf.fallbackToFollowerWithNewTerm(reply.Term)
	}
}

// the service using Raft (e.g. a k/v server) wants to start
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here (3A)
		// Check if a leader election should be started.

		rf.mu.Lock()

		switch rf.role {
		case Follower:
			if time.Since(rf.lastPing) > rf.electionTimeOut {
				rf.transitRole(Candidate)
				rf.lastElectionStart = time.Now()
				args := &RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: len(rf.log),
					LastLogTerm:  rf.getLastLogTerm(),
				}
				DPrintf("server %d start election term %d\n", rf.me, rf.currentTerm)
				rf.mu.Unlock()
				go rf.startElectionWithArgs(args)
			} else {
				rf.mu.Unlock()
			}
		case Candidate:
			if time.Since(rf.lastElectionStart) > rf.electionTimeOut {
				rf.currentTerm++
				rf.lastElectionStart = time.Now()
				args := &RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: len(rf.log),
					LastLogTerm:  rf.getLastLogTerm(),
				}
				DPrintf("server %d restarts election term %d\n", rf.me, rf.currentTerm)
				rf.mu.Unlock()
				go rf.startElectionWithArgs(args)
			} else {
				rf.mu.Unlock()
			}
		case Leader:
			args := &AppendEntriesArgs{
				Term:     rf.currentTerm,
				LeaderId: rf.me,
			}
			rf.mu.Unlock()
			rf.sendHeartBeatsWithArgs(args)
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 100)
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
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.electionTimeOut = time.Duration(500+(rand.Int63()%100)) * time.Millisecond

	rf.log = make([]LogEntry, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.transitRole(Follower)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
