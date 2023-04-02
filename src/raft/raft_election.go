package raft

import (
	"sync/atomic"
	"time"
)

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
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) MoreUpToDateThan(LastLogTerm int, lastLogIndex int) bool {
	if rf.getLastLogIndex() == 0 {
		return false
	}
	// lastLog := rf.logAt(rf.getLastLogIndex())
	// return (lastLog.CommandTerm == LastLogTerm && lastLog.CommandIndex > lastLogIndex) || (lastLog.CommandTerm > LastLogTerm)
	lastTerm := rf.getLastLogTerm()
	lastIndex := rf.getLastLogIndex()
	return (lastTerm == LastLogTerm && lastIndex > lastLogIndex) || (lastTerm > LastLogTerm)
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// restriction of log, if this.log more up-to-date, reject
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	// 什么时候grant？
	reply.Term = args.Term
	if args.Term > rf.currentTerm {
		rf.gotoTerm(args.Term)
		rf.persist()
	}
	if rf.MoreUpToDateThan(args.LastLogTerm, args.LastLogIndex) {
		reply.VoteGranted = false
		return
	}
	if rf.votedFor != -1 && rf.votedFor != args.CandidateID {
		reply.VoteGranted = false
	} else {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
		rf.state = Follower
		rf.persist()
		rf.resetElectionTimer()
	}
}

// func (rf *Raft) ChangeState(state int, refresh bool) {
// 	if state == Follower {
// 		rf.state = Follower
// 		// rf.votedFor = -1
// 		if refresh {
// 			rf.lastHeartBeat = getCurrentTime()
// 		}
// 	} else if state == Candidate { // 如果变身候选人 则开启一轮投票
// 		rf.state = Candidate
// 		rf.currentTerm++
// 	} else {
// 		rf.state = Leader
// 	}
// }

func (rf *Raft) ElectionRoutine() {
	needTicket := int32(len(rf.peers) / 2)
	for server := 0; server < len(rf.peers); server++ {
		if server == rf.me {
			continue
		}
		go func(server int) {
			rf.mu.Lock()
			args := RequestVoteArgs{
				Term:        rf.currentTerm, // 这个Term可能不是刚进来的term了，需要判断，传参时传入copy
				CandidateID: rf.me,
			}
			if rf.getLastLogIndex() > 0 {
				// lastLog := rf.logAt(rf.getLastLogIndex())
				args.LastLogIndex = rf.getLastLogIndex() // log 真tm应该单独搞个类维护，因为设计很多操作而不仅仅是个数组
				args.LastLogTerm = rf.getLastLogTerm()
			}
			rf.mu.Unlock()
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(server, &args, &reply)
			if ok {
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.gotoTerm(reply.Term)
					rf.state = Follower
					rf.persist()
					rf.resetElectionTimer()
					rf.mu.Unlock()
					return
				} else if reply.Term < rf.currentTerm || rf.state != Candidate {
					// 过时了
					rf.mu.Unlock()
					return
				}
				if reply.VoteGranted {
					if atomic.AddInt32(&needTicket, -1) <= 0 && rf.state != Leader {
						rf.state = Leader
						rf.ReInitLeader()
						rf.LeaderRoutine()
					}
				}
				rf.mu.Unlock()
			}
		}(server)
	}
}

func (rf *Raft) ElectionTicker() {
	// 随机睡一段时间醒来如果没有tick 开始选举
	for !rf.killed() {
		curTime := getCurrentTime()
		time.Sleep(time.Duration(randSleepTime(rf.me)) * time.Millisecond)
		rf.mu.Lock()
		if rf.lastHeartBeat < curTime && rf.state != Leader {
			if rf.state == Candidate {
				// 已经是候选人了 为什么还会再次选举？这不正常 应该继续等待选举结果
				rf.state = Follower
				rf.resetElectionTimer()
			} else {
				rf.state = Candidate
				rf.currentTerm++
				rf.votedFor = rf.me
				rf.persist()
				rf.ElectionRoutine()
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) AllServerRoutine() {
	// commitIndex > lastApplied
	rf.PrintCurState()
	// if len(rf.log) > 0 && rf.log[len(rf.log)-1].CommandIndex < rf.commitIndex {
	// 	DPrintf("fuck u %v, %v", rf.log, rf.commitIndex)
	// 	panic("fuck u not ")
	// }
	// if rf.commitIndex > rf.lastApplied {
	// 	// 搞一个不那么鲁棒的版本先，不如一步到位。
	// 	rf.lastApplied++
	// 	lastApplied := rf.lastApplied
	// 	go func() {
	// 		// bugpoint apply并且告诉客户端，如果leader的commitIndex++了，它的消息可能还没传给majority就寄了，应该等majority的commitIndex都承认了，然后才apply
	// 		rf.applyCh <- ApplyMsg{CommandValid: true, Command: rf.logAt(lastApplied).Command, CommandIndex: lastApplied}
	// 		DPrintf("Server%v apply cmd=%v with index=%v", rf.me, rf.logAt(lastApplied).Command, lastApplied)
	// 	}()
	// }
}

func (rf *Raft) Ticker() {
	// Server每隔一段时间就要操作
	for !rf.killed() {
		curTime := getCurrentTime()
		time.Sleep(time.Duration(ELECTION_TIMEOUT_MIN) * time.Millisecond)
		rf.mu.Lock()
		if rf.lastHeartBeat < curTime && rf.state == Leader {
			rf.LeaderRoutine()
		}

		rf.AllServerRoutine()

		rf.mu.Unlock()
	}
}
