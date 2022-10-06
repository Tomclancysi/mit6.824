package raft

import "time"

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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		// 说明有的地方准备投票新的了 那我也没必要heatbeat了
		reply.Success = false
		reply.Term = rf.currentTerm
	}
	if len(args.Entries) == 0 { // heatbeat

		if rf.currentTerm < args.Term { // 如果他是新的term，自然要跟随了，投它
			// rf.votedFor = args.LeaderId meiyong
			rf.currentTerm = args.Term
		}
		reply.Success = true
		reply.Term = rf.currentTerm

		// rf.lastHeartBeat = getCurrentTime()
		// rf.state = Follower
		rf.ChangeState(Follower, true)
	}
	rf.mu.Unlock()
	// TODO 5.3
	// ......
}

func (rf *Raft) LeaderRoutine() {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	// currentTerm := rf.currentTerm
	// eachTerm := make([]int, len(rf.peers))
	for server, _ := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int) {
			rf.mu.Lock()
			if rf.state != Leader {
				rf.mu.Unlock()
				return
			}
			args := AppendEntriesArgs{
				Term:     rf.currentTerm,
				LeaderId: rf.me,
				Entries:  make([]ApplyMsg, 0),
				// TODO
			}
			rf.mu.Unlock()

			reply := AppendEntriesReply{}
			if ok := rf.sendAppendEntries(server, &args, &reply); ok {
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.ChangeState(Follower, true)
				}
				rf.mu.Unlock()
			}
		}(server)
	}
}

func (rf *Raft) LeaderTicker() {
	// Leader 应该时刻在发heartbeat
	for rf.killed() == false {
		curTime := getCurrentTime()
		time.Sleep(time.Duration(ELECTION_TIMEOUT_MIN) * time.Millisecond)
		rf.mu.Lock()
		if rf.lastHeartBeat < curTime && rf.state == Leader {
			rf.LeaderRoutine()
		}
		rf.mu.Unlock()
	}
}