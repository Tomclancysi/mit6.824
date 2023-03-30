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
	L := len(rf.log)
	if L == 0 {
		return false
	}
	lastLog := rf.log[L-1]
	return (lastLog.CommandTerm == LastLogTerm && lastLog.CommandIndex > lastLogIndex) || (lastLog.CommandTerm > LastLogTerm)
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
		rf.currentTerm = args.Term
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
		rf.ChangeState(Follower, true)
	}
}

func (rf *Raft) ChangeState(state int, refresh bool) {
	if state == Follower {
		rf.state = Follower
		rf.votedFor = -1
		if refresh {
			rf.lastHeartBeat = getCurrentTime()
		}
	} else if state == Candidate { // 如果变身候选人 则开启一轮投票
		rf.state = Candidate
		rf.currentTerm++
	} else {
		rf.state = Leader
	}
}

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
			if len(rf.log) > 0 {
				args.LastLogIndex = rf.log[len(rf.log)-1].CommandIndex
				args.LastLogTerm = rf.log[len(rf.log)-1].CommandTerm
			}
			rf.mu.Unlock()
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(server, &args, &reply)
			if ok {
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.ChangeState(Follower, false)
					rf.mu.Unlock()
					return
				} else if reply.Term < rf.currentTerm {
					rf.mu.Unlock()
					return
				}
				if rf.state != Candidate {
					rf.mu.Unlock()
					return
				}
				if reply.VoteGranted {
					if atomic.AddInt32(&needTicket, -1) <= 0 && rf.state != Leader {
						rf.ChangeState(Leader, false)
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
				// 已经是候选人了 为什么还会再次选举？这不正常

				rf.ChangeState(Follower, false)
			} else {
				rf.ChangeState(Candidate, false)
				rf.ElectionRoutine()
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) AllServerRoutine() {
	// commitIndex > lastApplied
	rf.PrintCurState()
	if len(rf.log) > 0 && rf.log[len(rf.log)-1].CommandIndex < rf.commitIndex {
		DPrintf("fuck u %v, %v", rf.log, rf.commitIndex)
		panic("fuck u not ")
	}
	if rf.commitIndex > rf.lastApplied {
		rf.lastApplied++
		go func() {
			// apply并且告诉客户端
			// DPrintf("(AllServerRoutine)[%d,%d]SEND TO CLIENT %v, %v, isleader=%v", rf.me, rf.currentTerm, rf.log, rf.lastApplied-1, rf.state == Leader)
			rf.applyCh <- rf.log[rf.lastApplied-1]
		}()
	}
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
