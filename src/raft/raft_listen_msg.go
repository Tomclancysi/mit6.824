package raft

import "time"

// func (rf *Raft) ListenMessage() { // 不是这样用的
// 	for applyMsg := range rf.applyCh {
// 		rf.mu.Lock()
// 		// kick the start when get a command, use a goroutine to work it
// 		if applyMsg.CommandValid {
// 			go rf.Start(applyMsg.Command)
// 		}
// 		rf.mu.Unlock()
// 	}
// }

func (rf *Raft) ReachAggreement() {
	// 需要寻求半数成员的同意 看来还是在这实现好 如果半数以上统一那么me的index+1
	nextIndexOfMe := rf.log[len(rf.log)-1].CommandIndex
	needTicket := len(rf.peers)/2
	for server, _ := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int) { // 反复通信看能否达成广泛共识
			canFinish := false
			for canFinish == false {
				rf.mu.Lock()
				// rpc是异步请求的所以请求时应当注意条件是否满足
				if rf.state != Leader {
					return
				}
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: rf.nextIndex[server],
					PrevLogTerm:  rf.log[MaxInt(0, rf.nextIndex[server]-1)].CommandTerm,
					Entries:      rf.log[rf.nextIndex[server]:len(rf.log)], // 每次都把足够的指令发过去 发那些呢
					LeaderCommit: rf.commitIndex,
				}
				rf.mu.Unlock()
				reply := AppendEntriesReply{}

				if ok := rf.sendAppendEntries(server, &args, &reply); ok {
					rf.mu.Lock()
					if reply.Term > rf.currentTerm { // 它照样起到heartbeat作用
						rf.ChangeState(Follower, true)
						rf.mu.Unlock()
						return
					} else { // 检测是否成功如果成功就可以发送后面的Entries了
						if reply.Success {
							canFinish = true
							needTicket -= 1
							if needTicket == 0 {
								rf.nextIndex[rf.me] = MaxInt(nextIndexOfMe, rf.nextIndex[rf.me]) // commit 如何变动？
							}
							rf.matchIndex[server] = rf.nextIndex[server]
						} else {
							rf.nextIndex[server]--
						}
					}
					rf.mu.Unlock()
				}
			}
		}(server)
	}
}

func (rf *Raft) CommandAgreementTicker() {
	for rf.killed() == false {
		time.Sleep(time.Duration(getRand(rf.me))*time.Millisecond)
		rf.mu.Lock()
		if len(rf.log) > 0 && rf.nextIndex[rf.me] <= rf.log[len(rf.log)-1].CommandIndex {
			rf.mu.Unlock()
			rf.ReachAggreement()
		} else {
			rf.mu.Unlock()
		}
	}
}
