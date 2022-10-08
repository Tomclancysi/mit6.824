package raft

import (
	"time"
)

// 二进制指数退让
func (rf *Raft) BackOff(backOffTime int) int {
	backOffTime = MinInt(backOffTime*2, 1000)
	time.Sleep(time.Duration(backOffTime) * time.Millisecond)
	return backOffTime
}

func (rf *Raft) SendFeedToClientByChan(msg []ApplyMsg) {
	for _, entry := range msg {
		// DPrintf("[Server] Server%v Sending committed entry %v\n", rf.me, entry)
		rf.applyCh <- entry
	}
}

func (rf *Raft) GetReplicateArgsAndReply(server int, appendCMD bool) (AppendEntriesArgs, AppendEntriesReply) {
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex,
	}
	// 如果下一条记录在log中判断他们一不一样
	if rf.nextIndex[server] - 1 > 0 && len(rf.log) > 0 { // 判断这条记录是否对应啊
		args.PrevLogIndex = rf.nextIndex[server] - 1
		args.PrevLogTerm = rf.log[args.PrevLogIndex-1].CommandTerm
		// if rf.nextIndex[server] > len(rf.log) {
		// 	args.PrevLogIndex = rf.nextIndex[server] - 1
		// 	args.PrevLogTerm = rf.log[args.PrevLogIndex-1].CommandTerm
		// } else {
		// 	args.PrevLogIndex = rf.log[MaxInt(0, rf.nextIndex[server]-1)].CommandIndex
		// 	args.PrevLogTerm = rf.log[MaxInt(0, rf.nextIndex[server]-1)].CommandTerm
		// }
	}
	if appendCMD && rf.nextIndex[server]-1 < len(rf.log) { // replicate
		args.Entries = rf.log[rf.nextIndex[server]-1:] // 每次都把足够的指令发过去从确认项的下一项开始
	}
	reply := AppendEntriesReply{}
	return args, reply
}

func (rf *Raft) SendReplicate(server int, callback func(), retryTimes int) {
	canFinish := false
	haveSendAllCMD := false
	backOffTime := 100
	for canFinish == false || haveSendAllCMD == false {
		rf.mu.Lock()
		// rpc是异步请求的所以请求时应当注意条件是否满足
		if rf.state != Leader {
			return
		}
		args, reply := rf.GetReplicateArgsAndReply(server, canFinish)
		rf.mu.Unlock()

		if ok := rf.sendAppendEntries(server, &args, &reply); ok {
			rf.mu.Lock()
			if reply.Term > rf.currentTerm { // 它照样起到heartbeat作用
				rf.ChangeState(Follower, true)
				rf.mu.Unlock()
				return
			} else { // 检测是否成功如果成功就可以发送后面的Entries了
				if reply.Success {
					DPrintf("[REPLY] Follower%v Agree with the CMD Index%v Term%v, ENT=%v", server, args.PrevLogIndex, args.PrevLogTerm, args.Entries)
					if canFinish {
						haveSendAllCMD = true
						rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
					}
					canFinish = true
				} else {
					DPrintf("[REPLY] Follower%v REJECT the CMD Index%v Term%v, ENT=%v", server, args.PrevLogIndex, args.PrevLogTerm, args.Entries)
					rf.nextIndex[server]--
				}
			}
			rf.mu.Unlock()
		} else { // 如果掉线 类似二进制指数退让法 让他不要频繁的rpc
			backOffTime = rf.BackOff(backOffTime) // retry 的时候可能Leader已经寄了，说明agree还是得定时做一下
			if retryTimes <= 0 {
				return
			}
		}
	}
	callback()
}

func (rf *Raft) ReachAggreement() {
	// 需要寻求半数成员的同意 看来还是在这实现好 如果半数以上统一那么me的index+1
	// var wg sync.WaitGroup
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Leader%v>>>>>REACHAggreement %v, rf.log=%v", rf.me, rf.nextIndex, rf.log)
	nextIndexOfMe := rf.log[len(rf.log)-1].CommandIndex + 1
	needTicket := len(rf.peers)/2
	// DPrintf("[Server] leader%v@term%v reaching agreement of index%v\n", rf.me, rf.currentTerm, nextIndexOfMe-1)
	callback := func ()  {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		needTicket--
		if needTicket == 0 {
			if rf.nextIndex[rf.me] < nextIndexOfMe {
				go rf.SendFeedToClientByChan(rf.log[rf.nextIndex[rf.me]-1:nextIndexOfMe-1])
			}
			rf.nextIndex[rf.me] = MaxInt(nextIndexOfMe, rf.nextIndex[rf.me]) // commit 如何变动？
			rf.commitIndex = rf.nextIndex[rf.me] - 1
			// DPrintf("[Server] leader%v@term%v Sucessful reach agreement of index%v cmd%v\n", rf.me, rf.currentTerm, nextIndexOfMe-1, rf.log[nextIndexOfMe-2].Command)
		}
	}
	for server, _ := range rf.peers {
		if server == rf.me {
			continue
		}
		go rf.SendReplicate(server, callback, 0)
	}
}

func (rf *Raft) CheckIndex() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Leader%v>>>>>CHECKINDEX %v, rf.log=%v", rf.me, rf.nextIndex, rf.log)
	for server := 0; server < len(rf.peers); server++ {
		if server == rf.me {
			continue
		}
		go rf.SendReplicate(server, func() {}, 0)
		// if rf.nextIndex[server] < rf.commitIndex+1 {
		// 	DPrintf("[Leader%v] Server%v@term%v nextIndex%v < commitIndex%v\n", rf.me, server, rf.currentTerm, rf.nextIndex[server], rf.commitIndex)
		// 	go rf.SendReplicate(server, func() {}, 0)
		// }
	}
}

func (rf *Raft) CommandAgreementTicker() {
	for rf.killed() == false {
		time.Sleep(time.Duration(getRand(rf.me))*time.Millisecond)
		rf.mu.Lock()
		if rf.state == Leader {
			if len(rf.log) > 0 && rf.nextIndex[rf.me] <= rf.log[len(rf.log)-1].CommandIndex {
				rf.mu.Unlock()
				rf.ReachAggreement()
			} else {
				rf.mu.Unlock()
				rf.CheckIndex()
				time.Sleep(1000*time.Millisecond)
			}
		} else {
			rf.mu.Unlock()
		}
	}
}
