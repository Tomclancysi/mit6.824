package raft

import "sync"

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
			rf.votedFor = args.LeaderId
			rf.currentTerm = args.Term
		}
		reply.Success = true
		reply.Term = rf.currentTerm

		rf.lastHeartBeat = getCurrentTime()
		rf.state = Follower
	}
	rf.mu.Unlock()
	// TODO 5.3
	// ......
}

func (rf *Raft) LeaderRoutine() {
	var wg sync.WaitGroup
	rf.mu.Lock()
	defer rf.mu.Unlock()
	currentTerm := rf.currentTerm
	
	eachTerm := make([]int, len(rf.peers))
	for server, _ := range rf.peers {
		if server == rf.me {
			continue
		}
		wg.Add(1)
		go func(server int) {
			defer wg.Done()
			args := AppendEntriesArgs{
				Term:     currentTerm,
				LeaderId: rf.me,
				Entries:  make([]ApplyMsg, 0),
				// TODO
			}
			reply := AppendEntriesReply{}
			if ok := rf.sendAppendEntries(server, &args, &reply); ok {
				eachTerm[server] = reply.Term
			}
		}(server)
	}
	wg.Wait()
	maxTerm, replyCnt := 0, 0
	for _, t := range eachTerm {
		if maxTerm < t {
			maxTerm = t
		}
		if t > 0 {
			replyCnt++
		}
	}
	if maxTerm > rf.currentTerm || replyCnt == 0 {
		rf.votedFor = -1 // 已经过时了
		rf.state = Follower
	}
}