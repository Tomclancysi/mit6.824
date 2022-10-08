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

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.ChangeState(Follower, true)
	}
	reply.Term = rf.currentTerm
	if rf.MoreUpToDateThan(args.LastLogTerm, args.LastLogIndex) {
		reply.VoteGranted = false
		return
	}
	if rf.votedFor != -1 && rf.votedFor != args.CandidateID {
		reply.VoteGranted = false
	} else {
		// DPrintf("[Server] Server%v@Term%v vote for Server%v@Term%v\n", rf.me, rf.currentTerm, args.CandidateID, args.Term)
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
	}
}

func (rf *Raft) ChangeState(state int, refresh bool) {
	// stateName := [] string {"Follower", "Candidate", "Leader"}
	// DPrintf("[Server] Server%v@Term%v state change from %v to %v", rf.me, rf.currentTerm, stateName[rf.state], stateName[state])
	if state == Follower {
		rf.state = Follower
		rf.votedFor = -1
		if refresh {
			rf.lastHeartBeat = getCurrentTime()
		}
	} else if state == Candidate { // 如果变身候选人 则开启一轮投票
		rf.state = Candidate
		rf.currentTerm++
		rf.ElectionRoutine()
	} else {
		// DPrintf("[Server] Server%v@Term%v is select as Leader!!!\n", rf.me, rf.currentTerm)
		rf.state = Leader
	}
}

func (rf *Raft) ElectionRoutine() {
	needTicket := int32(len(rf.peers) / 2)
	for server := 0; server < len(rf.peers); server ++ {
		if server == rf.me {
			continue
		}
		go func(server int){
			rf.mu.Lock()
			args := RequestVoteArgs{
				Term: rf.currentTerm, // 这个Term可能不是刚进来的term了，需要判断，传参时传入copy
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
					if atomic.AddInt32(&needTicket, -1) == 0 && rf.state != Leader {
						rf.ChangeState(Leader, false)
						rf.ReInitLeader()
						rf.LeaderRoutine()
						DPrintf("New leader is %v@Term%v\n", rf.me, rf.currentTerm)
					}
				}
				rf.mu.Unlock()
			}
		}(server)
	}
}

func (rf *Raft) ElectionTicker() {
	// 随机睡一段时间醒来如果没有tick 开始选举
	for rf.killed() == false {
		curTime := getCurrentTime()
		time.Sleep(time.Duration(getRand(rf.me))*time.Millisecond)
		rf.mu.Lock()
		if rf.lastHeartBeat < curTime && rf.state != Leader {
			// rf.mu.Unlock()
			rf.ChangeState(Candidate, false)
		}
		rf.mu.Unlock()	
	}
}
