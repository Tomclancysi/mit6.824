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
		DPrintf("[Server] Server%v Sending committed entry %v\n", rf.me, entry)
		rf.applyCh <- entry
	}
}

func (rf *Raft) GetReplicateArgsAndReply(server int, appendCMD bool) (AppendEntriesArgs, AppendEntriesReply) {
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex,
	}
	// 如果nextIndex server就是最新的，那就没必要传这俩参数了
	if rf.nextIndex[server] > 0 && len(rf.log) > 0 && rf.nextIndex[server] <= len(rf.log) { // 判断这条记录是否对应啊
		args.PrevLogIndex = rf.nextIndex[server]
		// if rf.nextIndex[server]-1 >= len(rf.log) {
		// 	DPrintf("[Server ERROR] Server%v nextIndex[%v] = %v, len(rf.log) = %v\n", rf.me, server, rf.nextIndex[server], len(rf.log))
		// }
		args.PrevLogTerm = rf.log[MaxInt(0, rf.nextIndex[server]-1)].CommandTerm
	}
	if appendCMD { // replicate
		args.Entries = rf.log[rf.nextIndex[server]:] // 每次都把足够的指令发过去从确认项的下一项开始
	}
	reply := AppendEntriesReply{}
	return args, reply
}

func (rf *Raft) ReachAggreement() {
	// 需要寻求半数成员的同意 看来还是在这实现好 如果半数以上统一那么me的index+1
	// var wg sync.WaitGroup
	rf.mu.Lock()
	nextIndexOfMe := rf.log[len(rf.log)-1].CommandIndex + 1
	needTicket := len(rf.peers)/2
	DPrintf("[Server] leader%v@term%v reaching agreement of index%v\n", rf.me, rf.currentTerm, nextIndexOfMe-1)
	for server, _ := range rf.peers {
		if server == rf.me {
			continue
		}
		// wg.Add(1)
		// 当有新的时 走一趟流程
		go func(server int) { // 反复通信看能否达成广泛共识
			// defer wg.Done()
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
							if canFinish {
								haveSendAllCMD = true
								needTicket -= 1
							}
							canFinish = true
							rf.nextIndex[server] = MaxInt(rf.nextIndex[server], args.PrevLogIndex+len(args.Entries)+1)
							
							if needTicket == 0 {
								if rf.nextIndex[rf.me] < nextIndexOfMe {
									go rf.SendFeedToClientByChan(rf.log[rf.nextIndex[rf.me]-1:nextIndexOfMe-1])
								}
								rf.nextIndex[rf.me] = MaxInt(nextIndexOfMe, rf.nextIndex[rf.me]) // commit 如何变动？
								rf.commitIndex = rf.nextIndex[rf.me] - 1
								rf.matchIndex[server] = rf.nextIndex[server] - 1
								DPrintf("[Server] leader%v@term%v Sucessful reach agreement of index%v cmd%v\n", rf.me, rf.currentTerm, nextIndexOfMe-1, rf.log[nextIndexOfMe-2].Command)
							}
						} else {
							rf.nextIndex[server]--
						}
					}
					rf.mu.Unlock()
				} else { // 如果掉线 类似二进制指数退让法 让他不要频繁的rpc
					DPrintf("[Server]+++++ Server%v dead when reaching agreement\n", server)
					backOffTime = rf.BackOff(backOffTime) // retry 的时候可能Leader已经寄了，说明agree还是得定时做一下
					DPrintf("[Server]+++++ Server%v Retry\n", server)
				}
			}
		}(server)
	}
	rf.mu.Unlock()
	// wg.Wait() // 当这一轮所有的都搞好之后再开启下一轮
}

func (rf *Raft) CheckIndex() {
	for server := 0; server < len(rf.peers); server++ {
		if server == rf.me {
			continue
		}
		rf.mu.Lock()
		if rf.nextIndex[server] < rf.commitIndex+1 {
			rf.nextIndex[server] = rf.log[rf.nextIndex[server]-2].CommandIndex + 1
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) PeriodicallyCheckIndexTicker() {
	for rf.killed() == false {
		time.Sleep(time.Duration(getRand(rf.me))*time.Millisecond)
		rf.mu.Lock()
		if rf.state == Leader {
			rf.CheckIndex()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) CommandAgreementTicker() {
	for rf.killed() == false {
		time.Sleep(time.Duration(getRand(rf.me))*time.Millisecond)
		rf.mu.Lock()
		
		// 如果leader发现自己的log中最后一项大于commit index了,如果上一轮正在做，那么这样还是会立马开启一轮这就很离谱
		if rf.state == Leader && len(rf.log) > 0 && rf.nextIndex[rf.me] <= rf.log[len(rf.log)-1].CommandIndex {
			rf.mu.Unlock()
			rf.ReachAggreement()
		} else {
			rf.mu.Unlock()
		}
	}
}
