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
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
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
	Follower  = 0
	Candidate = 1
	Leader    = 2

	ELECTION_TIMEOUT_MAX = 300 // 修改这个确实会减少rpc次数
	ELECTION_TIMEOUT_MIN = 150
	ELECTION_TIMEOUT_INC = 30
)

func randSleepTime(server int) int {
	return ELECTION_TIMEOUT_MIN + rand.Intn((server+1)*ELECTION_TIMEOUT_INC)
}

func (rf *Raft) heartBeatsExperied() bool {
	return getCurrentTime()-rf.lastHeartBeat > ELECTION_TIMEOUT_MAX
}

func (rf *Raft) gotoTerm(target int) {
	rf.currentTerm = target
	rf.votedFor = -1
}

func (rf *Raft) gotoNextTerm() {
	rf.currentTerm++
	rf.votedFor = -1
}

func (rf *Raft) resetElectionTimer() {
	rf.lastHeartBeat = getCurrentTime()
}

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

// A Go object implementing a single Raft peer. 每一个peer就是这么一个对象
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers 这是啥
	persister *Persister          // Object to hold this peer's persisted state，只有currentTerm，log，commitIndex需要保存下来
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	log         []ApplyMsg

	// 当真正执行完这个指令之时，发送到管道中
	applyCh chan ApplyMsg

	// ---此为所有服务器上都有的---
	commitIndex int // 注意commit仅表示这一条指令被通过投票，准许执行
	lastApplied int // 这才表示机器真正执行结束了这条指令

	// ---only for leader 每次当选leader都要更改---
	nextIndex  []int // 初始值是Leader的logIndex+1 ！ 这个值不可以下降
	matchIndex []int // 初始为0，已知的复制到改服务器上的log，应该是为了处理哪些掉队的成员

	// my custom variable
	lastHeartBeat      int64
	state              int
	applyCond          *sync.Cond
	lastIncludingIndex int
	lastIncludingTerm  int
}

// --** time function **--
func getCurrentTime() int64 {
	return time.Now().UnixNano() / 1e6
}

// --** help function for log **--
func (rf *Raft) truncateIncBefore(index int) []ApplyMsg {
	index -= rf.lastIncludingIndex
	return rf.log[:index]
}

func (rf *Raft) truncateAfter(index int) []ApplyMsg {
	index -= rf.lastIncludingIndex
	return rf.log[index:]
}

func (rf *Raft) getLastLogIndex() int {
	if len(rf.log) == 0 {
		return rf.lastIncludingIndex
	} else {
		return rf.log[len(rf.log)-1].CommandIndex
	}
}

func (rf *Raft) getLastLogTerm() int {
	if len(rf.log) == 0 {
		return rf.lastIncludingTerm
	} else {
		return rf.log[len(rf.log)-1].CommandTerm
	}
}

func (rf *Raft) getLogTerm(index int) int {
	if index == rf.lastIncludingIndex {
		return rf.lastIncludingTerm
	}
	index -= rf.lastIncludingIndex
	return rf.log[index-1].CommandTerm
}

func (rf *Raft) applyLogs(logs []ApplyMsg) {
	for _, log := range logs {
		rf.applyCh <- log
	}
}

func (rf *Raft) logAt(index int) *ApplyMsg {
	if index-rf.lastIncludingIndex < 1 {
		panic(fmt.Sprintf("Maybe log has change to snapshot, visit Index=%v, lastIncludingIndex%v", index, rf.lastIncludingIndex))
	}
	index -= rf.lastIncludingIndex
	return &rf.log[index-1]
}

// --** raft state funciton **--

type RaftState struct {
	CurrentTerm        int
	VoteFor            int
	Log                []ApplyMsg
	LastIncludingIndex int
	LastIncludingTerm  int
}

func (rf *Raft) ReInitLeader() {
	var nextIndex int = rf.getLastLogIndex() + 1
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = nextIndex // 这俩啥意思
		rf.matchIndex[i] = 0
	}
	rf.commitIndex = len(rf.log) // 以往的都认为已经提交了
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	return term, isleader
}

func (rf *Raft) EncodeRaftStateSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	raftstate := RaftState{
		CurrentTerm:        rf.currentTerm,
		VoteFor:            rf.votedFor,
		Log:                rf.log,
		LastIncludingIndex: rf.lastIncludingIndex,
		LastIncludingTerm:  rf.lastIncludingTerm,
	}
	e.Encode(raftstate)
	// e.Encode(rf.currentTerm)
	// e.Encode(rf.votedFor)
	// e.Encode(rf.log)
	data := w.Bytes()
	return data
}

func (rf *Raft) DecodeRaftStateSnapshot(raftstate []byte) RaftState {
	r := bytes.NewBuffer(raftstate)
	d := labgob.NewDecoder(r)

	out := RaftState{}
	if d.Decode(&out) == nil {
		return out
	} else {
		panic("can not read state form bytes")
	}
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	rf.persister.SaveRaftState(rf.EncodeRaftStateSnapshot())
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	raftstate := rf.DecodeRaftStateSnapshot(data)
	rf.currentTerm = raftstate.CurrentTerm
	rf.votedFor = raftstate.VoteFor
	rf.log = raftstate.Log
	rf.lastIncludingIndex = raftstate.LastIncludingIndex
	rf.lastIncludingTerm = raftstate.LastIncludingTerm
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	// snapshotTerm := rf.logAt(index).CommandTerm
	// remove old log and 更新一些log相关的参数，这样访问才不会错

	// zhuyi!!!!!!!!!!!snapshot是来自应用层的，是在应用层上面进行一次覆盖。
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[SavingSnapshot]Server [%v] snapshoting, lastSnapIndex=%v, currentSnapIndex=%v", rf.me, rf.lastIncludingIndex, index)
	if index <= rf.lastIncludingIndex {
		return
	}

	rf.lastIncludingTerm = rf.logAt(index).CommandTerm
	rf.log = rf.truncateAfter(index)
	rf.lastIncludingIndex = index // Dangerous

	rf.persister.SaveStateAndSnapshot(rf.EncodeRaftStateSnapshot(), snapshot) // 其实这个snapshot指的是应用层的snapshot，而我的理解是state

	// 在这个点直接截断，那么如果想要访问以前的数据就不可能了，那什么时候snapshot呢，如何让其他部分能够

	// 似乎不需要对这个snapshot做什么操作啊

	// 向每一个follower传输这个snapshot,不用rpc而是用管道传输的，snapshot如何传输给其他raft？ 还是需要实现一个RPC的
	// go func() {
	// 	rf.applyCh <- ApplyMsg{
	// 		CommandValid:  false,
	// 		SnapshotValid: true,
	// 		Snapshot:      snapshot,
	// 		SnapshotTerm:  snapshotTerm,
	// 		SnapshotIndex: index}
	// }()
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
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 向raft提交一个事务，这个任务会被记在log中如果下一个周期就会告诉其他人
	// IMPORTANT因此Leader的周期需要做两件事，确认新的Command，修正其他follower的log
	if rf.state != Leader {
		return -1, -1, false
	}

	term := rf.currentTerm
	nextIndex := rf.getLastLogIndex() + 1 // 从1开始
	rf.log = append(rf.log, ApplyMsg{
		CommandValid: true,
		Command:      command,
		CommandIndex: nextIndex,
		CommandTerm:  rf.currentTerm,
		// default for 2D
	})
	rf.persist()
	return nextIndex, term, true
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
// func (rf *Raft) ticker() {
// for rf.killed() == false {
// 	time.Sleep(time.Duration(getRand(rf.me)) * time.Millisecond)

// 	_, isLeader := rf.GetState()
// 	if isLeader {
// 		rf.LeaderRoutine()
// 	} else {
// 		if rf.heartBeatsExperied() {
// 			rf.ElectionRoutine()
// 		}
// 	}

// }
// }

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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.votedFor = -1
	rf.currentTerm = 1
	rf.log = make([]ApplyMsg, 0)
	rf.applyCh = applyCh // 应该是从这里接收调用消息 看样子是个双向通道，读test源码看这个通道作用
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// my custom variable
	rf.lastHeartBeat = getCurrentTime()
	rf.state = Follower
	rf.applyCond = sync.NewCond(&rf.mu) // 果然如我所料，这里condition variable需要锁对象，这样wait的时候采能释放对应的锁。
	rf.lastIncludingIndex = 0
	// initialize from state persisted before a crash // 是不是要经常的persist
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ElectionTicker()
	go rf.Ticker()
	go rf.applier()
	// go rf.SnapshotTicker()

	return rf
}

func (rf *Raft) apply() {
	rf.applyCond.Broadcast()
}

func (rf *Raft) applier() {
	// 这个applier写的不够好，需要学习一下(这个大佬)[https://github.com/OneSizeFitsQuorum/MIT6.824-2021/blob/master/docs/lab2.md#%E5%BC%82%E6%AD%A5-applier-%E7%9A%84-exactly-once]，在不可靠的系统中，如何让一个命令只执行一次
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for !rf.killed() {
		// 2D改了这里也要改
		if rf.commitIndex > rf.lastApplied && rf.getLastLogIndex() > rf.lastApplied {
			rf.lastApplied++
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.logAt(rf.lastApplied).Command,
				CommandIndex: rf.lastApplied,
			}
			DPrintf("Server %v apply %v", rf.me, rf.lastApplied)
			rf.mu.Unlock()
			rf.applyCh <- applyMsg
			rf.mu.Lock()
		} else {
			rf.applyCond.Wait()
		}
	}
}
