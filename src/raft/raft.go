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

	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
)

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

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
	applyCh   chan ApplyMsg

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 3A
	// Persisted state on all servers
	currentTerm int
	votedFor    int
	log         []LogEntry

	// 3D
	snapshot          []byte
	lastIncludedIndex int
	lastIncludedTerm  int

	// Volatile state on all servers
	commitIndex       int
	lastApplied       int
	role              Role
	electionTimeOut   time.Duration
	lastPing          time.Time
	lastElectionStart time.Time
	leaderId          int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int
}

type LogEntry struct {
	Term    int
	Command interface{}
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
	writer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(writer)
	encoder.Encode(rf.currentTerm)
	encoder.Encode(rf.votedFor)
	encoder.Encode(rf.log)
	encoder.Encode(rf.lastIncludedIndex)
	encoder.Encode(rf.lastIncludedTerm)
	state := writer.Bytes()
	rf.persister.Save(state, rf.snapshot)
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
	reader := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(reader)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int
	if decoder.Decode(&currentTerm) != nil || decoder.Decode(&votedFor) != nil ||
		decoder.Decode(&log) != nil || decoder.Decode(&lastIncludedIndex) != nil ||
		decoder.Decode(&lastIncludedTerm) != nil {
		DPrintf("Decode error")
	} else {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
	}
}

// Helper for get log term - idx in rf.log
func (rf *Raft) getLogTerm(idx int) int {
	if idx == 0 || len(rf.log) == 0 {
		return 0
	}
	return rf.log[idx-1].Term
}

// Helper to get log length (counting snapshot)
func (rf *Raft) getLastLogIndex() int {
	return len(rf.log) + rf.lastIncludedIndex
}

// Helper to get last log term
func (rf *Raft) getLastLogTerm() int {
	if len(rf.log) == 0 {
		if rf.snapshot != nil {
			return rf.lastIncludedTerm
		} else {
			return 0
		}
	} else {
		return rf.log[len(rf.log)-1].Term
	}
}

func (rf *Raft) fallbackToFollowerWithNewTerm(newTerm int) {
	rf.mu.Lock()
	if newTerm <= rf.currentTerm {
		rf.mu.Unlock()
		return
	}
	rf.currentTerm = newTerm
	rf.votedFor = -1
	rf.transitRole(Follower)
	rf.persist()
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
		rf.persist()
	case Leader:
		rf.leaderId = rf.me
		rf.nextIndex = make([]int, len(rf.peers))
		for i := range rf.nextIndex {
			rf.nextIndex[i] = rf.getLastLogIndex() + 1
		}
		rf.matchIndex = make([]int, len(rf.peers))
	}
}

// Updates commit index and apply to state machine
func (rf *Raft) updateCommitIndexAndApply(newCommitIndex int, term int) {
	rf.mu.Lock()
	if newCommitIndex <= rf.commitIndex || newCommitIndex > rf.getLastLogIndex() || rf.currentTerm != term {
		rf.mu.Unlock()
		return
	}
	DPrintf("Update commit index in %d from %d to %d\n", rf.me, rf.commitIndex, newCommitIndex)
	rf.commitIndex = newCommitIndex
	rf.mu.Unlock()
	go rf.maybeApply()
}

func (rf *Raft) maybeApply() {
	rf.mu.Lock()
	if rf.lastApplied < rf.commitIndex {
		lastAppliedTrimmed := rf.lastApplied - rf.lastIncludedIndex
		rf.lastApplied++
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[lastAppliedTrimmed].Command,
			CommandIndex: rf.lastApplied,
		}
		rf.mu.Unlock()
		DPrintf("MaybeApply(%v)", msg)
		rf.applyCh <- msg
		rf.maybeApply()
	} else {
		rf.mu.Unlock()
	}
}

// Generate RequestVoteArgs for election - needs lock
func (rf *Raft) prepForElection() RequestVoteArgs {
	rf.transitRole(Candidate)
	rf.lastElectionStart = time.Now()
	return RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}
}

// Start election
func (rf *Raft) startElectionWithArgs(args *RequestVoteArgs) {
	votesChan := make(chan bool)
	for server := 0; server < len(rf.peers); server++ {
		if server == rf.me {
			continue
		}
		go rf.sendRequestVoteAndHandle(server, args, votesChan)
	}
	votes := 1
	for server := 0; server < len(rf.peers)-1; server++ {
		if <-votesChan {
			votes++
		}
		if votes > len(rf.peers)/2 {
			DPrintf("%d won election term %d\n", args.CandidateId, args.Term)
			rf.mu.Lock()
			if rf.role == Candidate && rf.currentTerm == args.Term {
				rf.transitRole(Leader)
				rf.sendHeartBeats()
			}
			rf.mu.Unlock()
			break
		}
	}
}

// As leader, send heartbeats to all peers - needs lock
func (rf *Raft) sendHeartBeats() {
	for server := 0; server < len(rf.peers); server++ {
		if server == rf.me {
			continue
		}
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			Entries:      make([]LogEntry, 0),
			LeaderCommit: rf.commitIndex,
			MatchIndex:   rf.matchIndex[server],
		}
		go rf.sendAppendEntriesAndHandle(server, args)
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	DPrintf("Snapshot is called on %d", rf.me)
	rf.mu.Lock()
	if index <= rf.lastIncludedIndex {
		rf.mu.Unlock()
		return
	}
	indexTrimmed := index - rf.lastIncludedIndex

	if len(rf.log) < indexTrimmed {
		rf.mu.Unlock()
		return
	}

	rf.snapshot = snapshot
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = rf.getLogTerm(indexTrimmed)
	rf.log = rf.log[indexTrimmed:]
	if index > rf.lastApplied {
		rf.lastApplied = index
	}
	if index > rf.commitIndex {
		rf.commitIndex = index
	}
	rf.mu.Unlock()
	DPrintf("Snapshot is finished on %d", rf.me)
}

// InstallSnapshot RPC
type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}

	if args.LastIncludedIndex < rf.lastIncludedIndex {
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
		rf.transitRole(Follower)
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	rf.lastPing = time.Now()
	rf.leaderId = args.LeaderId
	reply.Term = rf.currentTerm

	if rf.getLastLogIndex() > args.LastIncludedIndex {
		argsLastIndexTrimmed := args.LastIncludedIndex - rf.lastIncludedIndex
		if rf.getLogTerm(argsLastIndexTrimmed) == args.LastIncludedTerm {
			rf.log = rf.log[argsLastIndexTrimmed:]
		} else {
			rf.log = rf.log[:0]
		}
	} else {
		rf.log = rf.log[:0]
	}

	rf.snapshot = args.Data
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.persist()
	if args.LastIncludedIndex > rf.lastApplied {
		rf.lastApplied = args.LastIncludedIndex
	}
	if args.LastIncludedIndex > rf.commitIndex {
		rf.commitIndex = args.LastIncludedIndex
	}

	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      rf.snapshot,
		SnapshotTerm:  rf.lastIncludedTerm,
		SnapshotIndex: rf.lastIncludedIndex,
	}
	rf.mu.Unlock()
	DPrintf("InstallSnapshot(%v)", msg)
	rf.applyCh <- msg
}

func (rf *Raft) sendInstallSnapshotAndHandle(server int, args *InstallSnapshotArgs) {
	reply := &InstallSnapshotReply{}
	if !rf.peers[server].Call("Raft.InstallSnapshot", args, reply) {
		return
	}

	if reply.Term > args.Term {
		rf.fallbackToFollowerWithNewTerm(reply.Term)
	}

	rf.mu.Lock()
	if rf.currentTerm != args.Term || rf.role != Leader {
		rf.mu.Unlock()
		return
	}

	rf.matchIndex[server] = args.LastIncludedIndex
	rf.nextIndex[server] = args.LastIncludedIndex + 1
	rf.mu.Unlock()
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
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term == rf.currentTerm && rf.votedFor != -1 {
		reply.Term = rf.currentTerm
		reply.VoteGranted = (rf.votedFor == args.CandidateId)
		return
	}

	// Update current term and reset vote
	if args.Term > rf.currentTerm {
		rf.transitRole(Follower)
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}

	// Check if candidate's log is at least as up-to-date as the current instance
	willVote := false
	if args.LastLogTerm == rf.getLastLogTerm() {
		willVote = args.LastLogIndex >= rf.getLastLogIndex()
	} else {
		willVote = args.LastLogTerm > rf.getLastLogTerm()
	}

	// Update vote
	if willVote {
		rf.votedFor = args.CandidateId
		rf.persist()
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = willVote
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
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
	MatchIndex   int
}

type AppendEntriesReply struct {
	Term                   int
	Success                bool
	RejectedTermFirstIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	shouldTransitToFollower := false
	if rf.role == Candidate {
		shouldTransitToFollower = args.Term >= rf.currentTerm
	} else if rf.role == Leader {
		shouldTransitToFollower = args.Term > rf.currentTerm
	}

	if shouldTransitToFollower {
		rf.transitRole(Follower)
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}

	rf.lastPing = time.Now()
	rf.leaderId = args.LeaderId
	reply.Term = rf.currentTerm

	if rf.commitIndex < args.LeaderCommit && rf.commitIndex < args.MatchIndex {
		go rf.updateCommitIndexAndApply(min(rf.getLastLogIndex(), min(args.LeaderCommit, args.MatchIndex)), rf.currentTerm)
	}

	// Early return for heart beat
	if len(args.Entries) == 0 {
		reply.Success = true
		return
	}

	if args.PrevLogIndex > rf.getLastLogIndex() {
		reply.Success = false
		reply.RejectedTermFirstIndex = rf.getLastLogIndex()
		return
	}

	argsPrevLogIndexTrimmed := args.PrevLogIndex - rf.lastIncludedIndex

	if args.PrevLogTerm != rf.getLogTerm(argsPrevLogIndexTrimmed) {
		reply.Success = false
		rejectedTerm := rf.getLogTerm(argsPrevLogIndexTrimmed)
		for index := argsPrevLogIndexTrimmed; index > 0; index-- {
			if rf.getLogTerm(index) != rejectedTerm {
				break
			}
			reply.RejectedTermFirstIndex = index
		}
		return
	}

	DPrintf("Server %d before merge rf.log is %d: %v\n", rf.me, len(rf.log), rf.log)
	entryIndex := 0
	logCutIndex := argsPrevLogIndexTrimmed + 1
	mismatch := false
	for {
		if entryIndex >= len(args.Entries) || logCutIndex > len(rf.log) {
			break
		}
		if rf.getLogTerm(logCutIndex) != args.Entries[entryIndex].Term {
			mismatch = true
			break
		}
		entryIndex++
		logCutIndex++
	}
	if mismatch || len(args.Entries)+argsPrevLogIndexTrimmed > len(rf.log) {
		rf.log = append(rf.log[:argsPrevLogIndexTrimmed], args.Entries...)
		rf.persist()
	}
	DPrintf("Server %d After merge rf.log is %d: %v\n", rf.me, len(rf.log), rf.log)

	if args.LeaderCommit > rf.commitIndex {
		go rf.updateCommitIndexAndApply(min(args.LeaderCommit, rf.getLastLogIndex()), rf.currentTerm)
	}
	reply.Success = true
}

func (rf *Raft) sendAppendEntriesAndHandle(server int, args *AppendEntriesArgs) {
	reply := &AppendEntriesReply{}
	if !rf.peers[server].Call("Raft.AppendEntries", args, reply) {
		return
	}

	if !reply.Success && reply.Term > args.Term {
		rf.fallbackToFollowerWithNewTerm(reply.Term)
	}

	// Return for heartbeats
	if len(args.Entries) == 0 {
		return
	}

	rf.mu.Lock()
	if rf.currentTerm != args.Term || rf.role != Leader {
		rf.mu.Unlock()
		return
	}
	DPrintf("replication response for {%d, %d, command len %d} is %t from %d to %d\n", args.PrevLogIndex, args.PrevLogTerm, len(args.Entries), reply.Success, args.LeaderId, server)
	if reply.Success {
		newMatchIndex := args.PrevLogIndex + len(args.Entries)
		if rf.matchIndex[server] < newMatchIndex {
			rf.matchIndex[server] = newMatchIndex
			rf.nextIndex[server] = newMatchIndex + 1
		}
	} else {
		rf.nextIndex[server] = reply.RejectedTermFirstIndex
	}
	rf.mu.Unlock()
}

func (rf *Raft) startReplication(index int, term int) {
	committed := false
	for !rf.killed() {
		rf.mu.Lock()
		if rf.role != Leader || rf.currentTerm != term || index != rf.getLastLogIndex() {
			rf.mu.Unlock()
			break
		}
		synced := 0
		for server := 0; server < len(rf.peers); server++ {
			if server == rf.me || rf.matchIndex[server] >= index {
				synced++
				continue
			}
			prevIndex := rf.nextIndex[server] - 1
			prevIndexTrimmed := prevIndex - rf.lastIncludedIndex
			if prevIndexTrimmed < 0 {
				args := &InstallSnapshotArgs{
					Term:              rf.currentTerm,
					LeaderId:          rf.me,
					LastIncludedIndex: rf.lastIncludedIndex,
					LastIncludedTerm:  rf.lastIncludedTerm,
					Data:              rf.snapshot,
				}
				go rf.sendInstallSnapshotAndHandle(server, args)
			} else {
				args := &AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: prevIndex,
					PrevLogTerm:  rf.getLogTerm(prevIndexTrimmed),
					Entries:      append([]LogEntry(nil), rf.log[prevIndexTrimmed:]...),
					LeaderCommit: rf.commitIndex,
				}
				go rf.sendAppendEntriesAndHandle(server, args)
			}
		}
		rf.mu.Unlock()

		// all synced
		if synced > len(rf.peers)/2 && !committed {
			committed = true
			go rf.updateCommitIndexAndApply(index, term)
		}

		if synced == len(rf.peers) {
			break
		}

		// Sleep if not finished
		time.Sleep(time.Duration(200) * time.Millisecond)
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
	rf.mu.Lock()
	index := rf.getLastLogIndex()
	term := rf.currentTerm
	isLeader := rf.role == Leader
	DPrintf("Start for %v received by %d with {%d %d %t}\n", command, rf.me, index, term, isLeader)

	// Your code here (3B).
	if isLeader {
		rf.log = append(rf.log, LogEntry{Term: term, Command: command})
		index = rf.getLastLogIndex()
		rf.persist()
		rf.mu.Unlock()
		go rf.startReplication(index, term)
	} else {
		rf.mu.Unlock()
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
				args := rf.prepForElection()
				go rf.startElectionWithArgs(&args)
			}
		case Candidate:
			if time.Since(rf.lastElectionStart) > rf.electionTimeOut {
				args := rf.prepForElection()
				go rf.startElectionWithArgs(&args)
			}
		case Leader:
			rf.sendHeartBeats()
		}

		rf.mu.Unlock()

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
	rf.applyCh = applyCh

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.electionTimeOut = time.Duration(500+(rand.Int63()%100)) * time.Millisecond

	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0

	rf.log = make([]LogEntry, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.transitRole(Follower)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = persister.ReadSnapshot()

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
