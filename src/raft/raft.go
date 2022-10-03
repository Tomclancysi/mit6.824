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

	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.

// global const variable
const (
	ElectionTimeOut = 300
)

const (
	Follower  = 0
	Candidate = 1
	Leader    = 2
)

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

// A Go object implementing a single Raft peer. 每一个peer就是这么一个对象
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers 这是啥
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	log         []ApplyMsg

	commitIndex int
	lastApplied int

	// only for leader
	nextIndex  []int
	matchIndex []int

	// my custom variable
	lastHeartBeat int64
	state         int
}

// function get current time in millisecond
func getCurrentTime() int64 {
	return time.Now().UnixNano() / 1e6
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.votedFor == rf.me && rf.state == Leader
	return term, isleader
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

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
	VoteFor     int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		log.Printf("reject for low term cur=%v, req=%v\n", rf.currentTerm, args.Term)
	} else if args.Term == rf.currentTerm {
		// 每个tern一个server只能投票一次，投给谁之后不能改
		if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
			log.Printf("(Server %v, Term %v) Vote for (Server %v, Term %v)\n", rf.me, rf.currentTerm, args.CandidateID, args.Term)
			reply.VoteGranted = true
			rf.votedFor = args.CandidateID
		} else {
			reply.VoteGranted = false
		}
	} else if args.Term > rf.currentTerm {
		// 如果term比较大，那就任它当老大了
		log.Printf("(Server %v, Term %v) Vote for (Server %v, Term %v)\n", rf.me, rf.currentTerm, args.CandidateID, args.Term)
		reply.VoteGranted = true
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateID
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []ApplyMsg
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if args.Term < rf.currentTerm {
		// 说明有的地方准备投票新的了 那我也没必要heatbeat了
		reply.Success = false
		reply.Term = rf.currentTerm
	}
	if len(args.Entries) == 0 { // heatbeat
		if rf.currentTerm < args.Term { // 如果他是新的term，自然要跟随了，投它
			rf.votedFor = args.LeaderId
		}
		reply.Success = true
		reply.Term = rf.currentTerm

		rf.lastHeartBeat = getCurrentTime()
		rf.state = Follower
		return
	}
	// TODO 5.3
	// ......
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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

	// Your code here (2B).

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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {

	for rf.killed() == false {

		_, isLeader := rf.GetState()
		if isLeader {
			for server, _ := range rf.peers {
				if server == rf.me {
					continue
				}
				args := AppendEntriesArgs{
					Term:     rf.currentTerm,
					LeaderId: rf.me,
					Entries:  make([]ApplyMsg, 0),
				}
				reply := AppendEntriesReply{}
				if ok := rf.sendAppendEntries(server, &args, &reply); ok {
					// 收到回复之后，比较Term
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.votedFor = -1 // 已经过时了
						rf.state = Follower
						break
					}
				}
			}
		} else {
			if getCurrentTime()-rf.lastHeartBeat > ElectionTimeOut {
				// 变成候选人，疯狂拉人投票中
				// term如何增加的呢，如果一直失败那么term不应该一直增，只能增加一次罢了
				if rf.state == Follower {
					rf.state = Candidate
					rf.votedFor = -1 // 不能给自己投票的
					rf.currentTerm++
				} else {
					// crush happend 发生过碰撞
					rf.currentTerm++
				}
				electionSuccess := true
				ticket, allTicket := 0, 1
				for server, _ := range rf.peers {
					if server == rf.me {
						continue
					}
					args := RequestVoteArgs{
						Term:        rf.currentTerm,
						CandidateID: rf.me,
					}
					reply := RequestVoteReply{}
					if ok := rf.sendRequestVote(server, &args, &reply); ok {
						allTicket++
						if reply.VoteGranted {
							ticket++
						} else if rf.currentTerm < reply.Term {
							rf.currentTerm = reply.Term
							electionSuccess = false
							break
						}
					}
				}
				log.Printf("(Server %v, Term %v) earn %v tickets of all %v\n", rf.me, rf.currentTerm, ticket, allTicket)
				electionSuccess = electionSuccess && (ticket > allTicket/2)
				if electionSuccess {
					log.Printf("==>(Server %v, Term %v) is select as leader\n", rf.me, rf.currentTerm)
					rf.votedFor = rf.me
					rf.state = Leader
				} else {
					// rf.state = Follower
					// 可能这轮投票起了冲突，重新投票term轮次+1
					time.Sleep(time.Duration(rand.Int63()%100+200) * time.Millisecond)
				}
			}
		}
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
	// peers包含所有peer的rpc包括自己， persister是个单例对象，全局所有peer共享的状态，chan管道是收ApplyMsg的
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.votedFor = -1
	rf.currentTerm = 0
	// my custom variable
	rf.lastHeartBeat = getCurrentTime()
	rf.state = Follower
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
