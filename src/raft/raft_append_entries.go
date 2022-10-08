package raft

import (
	"math/rand"
	"time"
)

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

func (rf *Raft) JoinEntries(others []ApplyMsg) {
	lastIndex := 1
	if len(others) == 0 {
		return
	}
	if len(rf.log) > 0 {
		lastIndex = rf.log[len(rf.log)-1].CommandIndex + 1
	}
	for _, cmd := range others {
		if cmd.CommandIndex == lastIndex {
			lastIndex ++
			rf.log = append(rf.log, cmd)
		}
	}
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
		if args.PrevLogIndex <= len(rf.log) {
			rf.log = rf.log[:args.PrevLogIndex-1] // 删除掉不agree的部分
		}
	} else {
		// DPrintf("[Server] Follower accept these log entries %v\n",args.Entries)
		rf.JoinEntries(args.Entries)
	}
	
	if args.LeaderCommit > rf.commitIndex && len(rf.log) > 0 {
		n := rf.log[len(rf.log)-1].CommandIndex // log不一定有值把
		N := MinInt(n, args.LeaderCommit)
		go rf.SendFeedToClientByChan(rf.log[rf.commitIndex:N]) // 把commit范围内的执行了
		rf.commitIndex = N
	}
	// 随机数小于0.1 则print
	if rand.Float64() < 0.1 {
		DPrintf("[Follower] Server%v commitIndex is %v, the log is %v", rf.me, rf.commitIndex, rf.log)
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