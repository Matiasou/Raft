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

	"cs350/labgob"
	"cs350/labrpc"
)

// import "bytes"
// import "cs350/labgob"

// as each Raft peer becomes aware that successive log entries are
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

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm   int
	votedFor      int
	voteCounter   int // to count the number of votes a server recieves. if voteCounter > N / 2 then its the leader
	state         int // for follower depending on # if its a follower, leader or a candidate
	heartBeat     bool
	logEntries    []LogEntry
	nextIndex     []int
	matchIndex    []int
	lastApplied   int
	commitIndex   int
	applyCh       chan ApplyMsg
	applyChBuffer chan ApplyMsg
}

type LogEntry struct { // struct for entries
	Entry interface{} // log interface
	Term  int         // term in log
	Index int         // universal index of the log
}

type AppendEntryArgs struct {
	Term         int // current term of the leader
	LeaderId     int // id of the leader
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntryReply struct {
	Term    int  // current term of the receiving node
	Success bool // AppendEntry declined or accepted
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term     int
	Success  bool
	LeaderId int
}

const (
	Follower_State  = 0
	Candidate_State = 1
	Leader_State    = 2
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = (rf.state == Leader_State)
	rf.mu.Unlock()
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logEntries)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var votedFor int
	var currentTerm int
	var logEntries []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logEntries) != nil {
		panic("Big Issue")
	} else {
		rf.votedFor = votedFor
		rf.currentTerm = currentTerm
		rf.logEntries = logEntries
		rf.persist()
	}
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	// Done want it to always return true
	return true
}

func (rf *Raft) SendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ok {
		return ok
	}
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = Follower_State
		rf.votedFor = -1
		rf.persist()
		return ok
	}
	if rf.state != Leader_State || rf.currentTerm != args.Term {
		return ok
	}
	if reply.Success {
		rf.nextIndex[server] = args.LastIncludedIndex + 1
		rf.matchIndex[server] = args.LastIncludedIndex
	}

	return ok
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower_State
		rf.votedFor = -1
		rf.persist()
	}

	rf.heartBeat = true
	// if args.LastIncludedIndex <= rf.lastSnapshotIndex {
	// 	reply.Term = rf.currentTerm
	// 	return
	// } else {
	// 	if args.LastIncludedIndex < rf.logEntries[0].Index {
	// 		if rf.logEntries[rf.LogIndexPosition(args.LastIncludedIndex)].Term != args.LastIncludedTerm {
	// 			rf.logEntries = make([]LogEntry, 0)
	// 		} else {
	// 			leftLog := make([]LogEntry, rf.logEntries[0].Index-args.LastIncludedIndex)
	// 			copy(leftLog, rf.logEntries[rf.LogIndexPosition(args.LastIncludedIndex)+1:])
	// 			rf.logEntries = leftLog
	// 		}
	// 	} else {
	// 		rf.logEntries = make([]LogEntry, 0)
	// 	}
	// }

	rf.applyChBuffer <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}

	rf.logEntries = []LogEntry{{nil, args.LastIncludedTerm, args.LastIncludedIndex}}

	rf.commitIndex = args.LastIncludedIndex
	reply.Success = true

	// persist the new log and states
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logEntries)
	state_data := w.Bytes()

	rf.persister.SaveStateAndSnapshot(state_data, args.Data)

}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.logEntries[0].Index {
		return
	} else if index > rf.commitIndex {
		return
	}

	// Trim the log
	rf.logEntries = rf.logEntries[index-rf.logEntries[0].Index:]

	// rf.applyChBuffer <- ApplyMsg{
	// 	SnapshotValid: true,
	// 	Snapshot:      snapshot,
	// 	SnapshotTerm:  rf.logEntries[0].Term,
	// 	SnapshotIndex: rf.logEntries[0].Index,
	// }

	// rf.commitIndex = rf.logEntries[0].Index

	// persist the new log and states
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logEntries)
	state_data := w.Bytes()

	rf.persister.SaveStateAndSnapshot(state_data, snapshot)

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // current term of the candidate
	CandidateId  int // candidate id, 0 through  N-1  N = total servers
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // current term of the receiving node
	VoteGranted bool // yes or no
}

func (rf *Raft) getLastTerm() int {
	return rf.logEntries[len(rf.logEntries)-1].Term
	// if len(rf.logEntries) > 0 {
	// 	lastIndex := len(rf.logEntries) - 1
	// 	return rf.logEntries[lastIndex].Term
	// } else {
	// 	return -1
	// }
}

func (rf *Raft) getLastIndex() int {
	return rf.logEntries[len(rf.logEntries)-1].Index
	// if len(rf.logEntries) > 0 {
	// 	lastIndex := len(rf.logEntries) - 1
	// 	return lastIndex
	// } else {
	// 	return -1
	// }
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	if rf.killed() {
		return
	}

	// Your code here (2A, 2B).
	// reply.Term = args.Term
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	lastLogIndex := rf.getLastIndex()
	lastLogTerm := rf.getLastTerm()

	if args.Term > rf.currentTerm {
		rf.state = Follower_State
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.voteCounter = 0
		rf.persist()
		//
	}
	if rf.currentTerm > args.Term {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		//
		return
	}
	//
	if rf.currentTerm == args.Term &&
		(rf.votedFor == -1) &&
		(args.LastLogTerm > lastLogTerm ||
			(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) { // good
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.persist()
		//
	}
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	if rf.killed() {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Success = true

	reply.Term = args.Term //rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm

		return
	} else if args.Term > rf.currentTerm {
		rf.state = Follower_State
		rf.votedFor = -1
		rf.voteCounter = 0
		rf.currentTerm = args.Term
		rf.persist()
	}
	if args.Term >= rf.currentTerm {
		rf.heartBeat = true
	}

	if args.PrevLogIndex >= 1 {

		if args.PrevLogIndex < rf.logEntries[0].Index {
			// for unreliable test (leader might not have received my installSS reply)
			reply.Success = false
			return
		}

		if args.PrevLogIndex > rf.logEntries[len(rf.logEntries)-1].Index {
			reply.Success = false

			return
		} else if args.PrevLogTerm != rf.logEntries[args.PrevLogIndex-rf.logEntries[0].Index].Term {
			reply.Success = false
			rf.persist()
			//
			return
		} else {
			rf.logEntries = rf.logEntries[:args.PrevLogIndex+1-rf.logEntries[0].Index]
			rf.logEntries = append(rf.logEntries, args.Entries...)
			reply.Success = true
			rf.persist()
		}
	} else if args.PrevLogIndex == 0 {
		rf.logEntries = []LogEntry{{0, 0, 0}}
		rf.logEntries = append(rf.logEntries, args.Entries...)
		reply.Success = true
		rf.persist()
	} else if args.PrevLogIndex == -1 {
		rf.logEntries = args.Entries
		reply.Success = true
		rf.persist()

	} else if len(args.Entries) != 0 {
		rf.logEntries = append(rf.logEntries, args.Entries...)
		reply.Success = true
		rf.persist()
	}
	if args.LeaderCommit > rf.commitIndex {
		N := min(args.LeaderCommit, args.PrevLogIndex+len(rf.logEntries))

		for i := rf.commitIndex + 1; i <= N; i++ {
			rf.commitIndex = i

			rf.applyChBuffer <- ApplyMsg{
				CommandValid: true,
				Command:      rf.logEntries[i-rf.logEntries[0].Index].Entry,
				CommandIndex: i,
			}

		}
	}
}
func min(x int, y int) int {
	if x < y {
		return x
	} else {
		return y
	}
}
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	//
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) { // Create HeartBeat similar to sendRequestVote
}
func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)

	if ok { //
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = Follower_State
			rf.voteCounter = 0
			rf.votedFor = -1
			rf.persist()
			rf.mu.Unlock()
			return ok
		} else if !reply.Success {
			rf.nextIndex[server] = 1
			//
			rf.mu.Unlock()
			return ok
		}
		//if i recieved success
		// what happens if success NEED TO DO ( call commit_checker )
		rf.matchIndex[server] = len(args.Entries) + args.PrevLogIndex
		rf.nextIndex[server] = rf.matchIndex[server] + 1
		rf.mu.Unlock()
		// log.Println("After send append entry", server, rf.matchIndex[server], rf.nextIndex[server])
		rf.commit_Checker()
	}
	return ok
}

func (rf *Raft) commit_Checker() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// log.Printf("[%d] Commit_Checker running\n", rf.me)

	overHalf := len(rf.peers) / 2
	for N := rf.commitIndex + 1; N <= rf.logEntries[len(rf.logEntries)-1].Index; N++ {
		counter := 0
		for _, index := range rf.matchIndex {
			if index >= N {
				counter++
			}
			if counter > overHalf {
				break
			}
		}
		if counter > overHalf && rf.logEntries[N-rf.logEntries[0].Index].Term == rf.currentTerm {
			for i := rf.commitIndex + 1; i <= N; i++ {
				rf.commitIndex = i

				rf.applyChBuffer <- ApplyMsg{
					CommandValid: true,
					Command:      rf.logEntries[i-rf.logEntries[0].Index].Entry,
					CommandIndex: i,
				}

			}
			break
		}
	}
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == Leader_State {
		term = rf.currentTerm
		isLeader = true
		index = rf.logEntries[len(rf.logEntries)-1].Index + 1
		l := LogEntry{
			Term:  term,
			Entry: command,
			Index: index,
		}
		rf.logEntries = append(rf.logEntries, l)
		rf.matchIndex[rf.me] = index
		rf.nextIndex[rf.me] = index + 1
		rf.persist()
		//
		return index, term, isLeader
	}
	// Your code here (2B).
	isLeader = false

	return index + 1, term, isLeader
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) sendServerVote(server int) {
	rf.mu.Lock()
	if rf.state != 1 || rf.killed() {
		rf.mu.Unlock()
		return
	}
	//

	if server != rf.me {
		//
		args := RequestVoteArgs{
			CandidateId: rf.me,
			Term:        rf.currentTerm}

		args.LastLogIndex = rf.logEntries[len(rf.logEntries)-1].Index
		args.LastLogTerm = rf.logEntries[len(rf.logEntries)-1].Term

		reply := RequestVoteReply{}
		rf.mu.Unlock()
		ok := rf.sendRequestVote(server, &args, &reply)
		rf.mu.Lock()

		if !ok {
			rf.mu.Unlock()
			//
			return
		}
		if rf.state != Candidate_State {
			rf.mu.Unlock()
			//
			return
		}

		if reply.VoteGranted {
			rf.voteCounter++
		} else if reply.Term > rf.currentTerm {
			//
			rf.currentTerm = reply.Term
			rf.state = 0
			rf.voteCounter = 0
			rf.votedFor = -1
			rf.persist()
		}
	}
	if rf.voteCounter > int(len(rf.peers)/2) {

		rf.state = 2
		rf.voteCounter = 0
		// have to reset for loop to reinitialized matchIndex to 0 and nextIndex to initialized to leader last log index + 1
		for i := 0; i < len(rf.nextIndex); i++ {
			rf.nextIndex[i] = rf.logEntries[len(rf.logEntries)-1].Index + 1
			rf.matchIndex[i] = 0
		}

		go rf.handleLeader()
	}
	rf.mu.Unlock()
}

func (rf *Raft) heartBeatHandler(server int) { // sending append entries
	rf.mu.Lock()

	if rf.nextIndex[server] > rf.logEntries[0].Index {
		deep_copy_log := make([]LogEntry, len(rf.logEntries))
		copy(deep_copy_log, rf.logEntries)

		args := AppendEntryArgs{
			LeaderId:     rf.me,
			Term:         rf.currentTerm,
			LeaderCommit: rf.commitIndex,
		}
		if rf.nextIndex[server] <= 0 {
			args.Entries = deep_copy_log
			args.PrevLogIndex = -1
			rf.nextIndex[server] = 0
		} else if rf.nextIndex[server] > rf.logEntries[len(rf.logEntries)-1].Index {
			args.Entries = []LogEntry{}
			args.PrevLogIndex = rf.nextIndex[server] - 1
			args.PrevLogTerm = rf.logEntries[len(rf.logEntries)-1].Term
		} else {
			args.Entries = deep_copy_log[rf.nextIndex[server]-rf.logEntries[0].Index:]
			args.PrevLogIndex = rf.nextIndex[server] - 1
			if args.PrevLogIndex >= 1 {
				args.PrevLogTerm = deep_copy_log[args.PrevLogIndex-rf.logEntries[0].Index].Term
			}
		}

		reply := AppendEntryReply{}
		rf.mu.Unlock()
		ok := rf.sendAppendEntry(server, &args, &reply)
		rf.mu.Lock()
		if !ok || rf.state != 2 {
			//
			rf.mu.Unlock()
			return
		}
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = 0
			rf.voteCounter = 0
			rf.votedFor = -1
			rf.persist()
			//
		}
		// if reply = sucess
		rf.mu.Unlock()
	} else {
		// follower is lagging, send the whole snapshot first

		SnapshotArgs := InstallSnapshotArgs{
			Term:              rf.currentTerm,
			LeaderId:          rf.me,
			LastIncludedIndex: rf.logEntries[0].Index,
			LastIncludedTerm:  rf.logEntries[0].Term,
			Data:              rf.persister.ReadSnapshot(),
		}
		SnapshotReply := InstallSnapshotReply{}
		rf.mu.Unlock()
		go rf.SendInstallSnapshot(server, &SnapshotArgs, &SnapshotReply)
	}
}

func (rf *Raft) handleLeader() {
	for !rf.killed() {
		rf.mu.Lock()

		// "\n              log =", (rf.logEntries)
		if rf.state != 2 {
			rf.mu.Unlock()
			return
		}
		for peer := range rf.peers {
			if peer != rf.me {
				go rf.heartBeatHandler(peer)
			}
		}
		rf.mu.Unlock()
		time.Sleep(90 * time.Millisecond)
	}

}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		electionTime := time.Duration(rand.Intn(901)+900) * time.Millisecond
		time.Sleep(electionTime)
		rf.mu.Lock()
		// log.Printf("[%d] commitIndex: %d", rf.me, rf.commitIndex)
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		if rf.heartBeat || rf.state == 2 {
			rf.heartBeat = false
		} else {

			rf.state = 1
			rf.currentTerm++
			rf.votedFor = rf.me
			rf.voteCounter = 1
			rf.persist()
			for peer := range rf.peers {
				go rf.sendServerVote(peer)
			}

		}
		rf.mu.Unlock()
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

	// Your initialization code here (2A, 2B, 2C).
	// rf.logEntries =
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = 0
	rf.voteCounter = 0
	rf.heartBeat = false
	// Your initialization code here 2B
	rf.lastApplied = 0
	rf.commitIndex = 0
	rf.logEntries = []LogEntry{{0, 0, 0}}

	rf.nextIndex = make([]int, len(rf.peers))

	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = len(rf.logEntries)
	}
	rf.matchIndex = make([]int, len(rf.peers))

	rf.applyCh = applyCh
	rf.applyChBuffer = make(chan ApplyMsg, 1000)

	applyMessege := ApplyMsg{
		CommandValid: true,
		Command:      rf.logEntries[0].Entry,
		CommandIndex: 0,
	}
	rf.applyChBuffer <- applyMessege
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	for i, _ := range rf.nextIndex {
		rf.nextIndex[i] = rf.logEntries[len(rf.logEntries)-1].Index + 1
	}

	if rf.commitIndex < rf.logEntries[0].Index {
		rf.commitIndex = rf.logEntries[0].Index

		rf.applyChBuffer <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      rf.persister.ReadSnapshot(),
			SnapshotTerm:  rf.logEntries[0].Term,
			SnapshotIndex: rf.logEntries[0].Index,
		}
	}

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.sendBufferToApplyCh()

	return rf
}

func (rf *Raft) sendBufferToApplyCh() {
	for !rf.killed() {
		applyMessege := <-rf.applyChBuffer
		rf.applyCh <- applyMessege
	}
}
