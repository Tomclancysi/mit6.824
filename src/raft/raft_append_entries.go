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
	if rf.currentTerm < args.Term { // 如果他是新的term，自然要跟随了，投它
		rf.currentTerm = args.Term
	}
	reply.Success = true
	reply.Term = rf.currentTerm
	rf.ChangeState(Follower, true)
	// 判断指定位置的Term Index相同返回true
	if args.PrevLogIndex > 0 && (args.PrevLogIndex > len(rf.log) || rf.log[args.PrevLogIndex-1].CommandTerm != args.PrevLogTerm) {
		reply.Success = false
		rf.log = rf.log[:args.PrevLogIndex-1] // 删除掉不agree的部分
	} else {
		// DPrintf("[Server] Follower accept these log entries %v\n",args.Entries)
		if len(args.Entries) > 0 {
			rf.log = append(rf.log, args.Entries...)
		}
	}
	
	if args.LeaderCommit > rf.commitIndex {
		n := rf.log[len(rf.log)-1].CommandIndex // log不一定有值把
		N := MinInt(n, args.LeaderCommit)
		go rf.SendFeedToClientByChan(rf.log[rf.commitIndex:N]) // 把commit范围内的执行了
		rf.commitIndex = N
	}
	rf.mu.Unlock()
}

func (rf *Raft) LeaderRoutine() {
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
			// args := AppendEntriesArgs{
			// 	Term:     rf.currentTerm,
			// 	LeaderId: rf.me,
			// 	//Entries:  rf.log[rf.matchIndex[server]:rf.nextIndex[server]],
			// 	LeaderCommit: rf.commitIndex,
			// 	// 如果没有猜错应该是这个吧 存疑 heartbeat 不要管这些！
			// 	PrevLogIndex: rf.log[rf.matchIndex[server]].CommandIndex,
			// 	PrevLogTerm: rf.log[rf.matchIndex[server]].CommandTerm,
			// }
			args, reply := rf.GetReplicateArgsAndReply(server, false)
			rf.mu.Unlock()

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