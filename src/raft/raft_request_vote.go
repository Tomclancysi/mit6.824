package raft

// import (
// 	"time"
// )

// // 二进制指数退让
// func (rf *Raft) BackOff(backOffTime int) int {
// 	backOffTime = MinInt(backOffTime*2, 1000)
// 	time.Sleep(time.Duration(backOffTime) * time.Millisecond)
// 	return backOffTime
// }

// func (rf *Raft) SendFeedToClientByChan(msg []ApplyMsg) {
// 	for _, entry := range msg {
// 		rf.applyCh <- entry
// 	}
// }

// func (rf *Raft) SendReplicate(server int, callback func(), retryTimes int) {
// 	// Leader 发送端逻辑
// 	canFinish := false
// 	haveSendAllCMD := false
// 	backOffTime := 100
// 	for canFinish == false || haveSendAllCMD == false {
// 		rf.mu.Lock()

// 		// rpc是异步请求的所以请求时应当注意条件是否满足
// 		if rf.state != Leader {
// 			return
// 		}
// 		args, reply := rf.GetReplicateArgsAndReply(server, canFinish)
// 		rf.mu.Unlock()

// 		if ok := rf.sendAppendEntries(server, &args, &reply); ok {
// 			rf.mu.Lock()
// 			if reply.Term > rf.currentTerm { // 它照样起到heartbeat作用
// 				rf.ChangeState(Follower, true)
// 				rf.mu.Unlock()
// 				return
// 			} else { // 检测是否成功如果成功就可以发送后面的Entries了
// 				if reply.Success {
// 					if canFinish {
// 						haveSendAllCMD = true
// 					}
// 					canFinish = true
// 					rf.nextIndex[server] = MaxInt(rf.nextIndex[server], args.PrevLogIndex+len(args.Entries)+1)
// 				} else {
// 					rf.nextIndex[server]--
// 				}
// 			}
// 			rf.mu.Unlock()
// 		} else { // 如果掉线 类似二进制指数退让法 让他不要频繁的rpc
// 			backOffTime = rf.BackOff(backOffTime) // retry 的时候可能Leader已经寄了，说明agree还是得定时做一下
// 			if retryTimes <= 0 {
// 				return
// 			}
// 		}
// 	}
// 	callback()
// }

// func (rf *Raft) ReachAggreement() {
// 	// 需要寻求半数成员的同意 看来还是在这实现好 如果半数以上统一那么me的index+1
// 	// var wg sync.WaitGroup
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()
// 	nextIndexOfMe := rf.log[len(rf.log)-1].CommandIndex + 1
// 	needTicket := len(rf.peers) / 2

// 	callback := func() {
// 		rf.mu.Lock()
// 		defer rf.mu.Unlock()
// 		needTicket--
// 		if needTicket == 0 {
// 			if rf.nextIndex[rf.me] < nextIndexOfMe {
// 				go rf.SendFeedToClientByChan(rf.log[rf.nextIndex[rf.me]-1 : nextIndexOfMe-1])
// 			}
// 			rf.nextIndex[rf.me] = MaxInt(nextIndexOfMe, rf.nextIndex[rf.me]) // commit 如何变动？
// 			rf.commitIndex = rf.nextIndex[rf.me] - 1

// 		}
// 	}
// 	for server, _ := range rf.peers {
// 		if server == rf.me {
// 			continue
// 		}
// 		go rf.SendReplicate(server, callback, 0)
// 		// wg.Add(1)
// 		// 当有新的时 走一趟流程
// 		// go func(server int) { // 反复通信看能否达成广泛共识
// 		// 	// defer wg.Done()
// 		// 	canFinish := false
// 		// 	haveSendAllCMD := false
// 		// 	backOffTime := 100
// 		// 	for canFinish == false || haveSendAllCMD == false {
// 		// 		rf.mu.Lock()
// 		// 		// rpc是异步请求的所以请求时应当注意条件是否满足
// 		// 		if rf.state != Leader {
// 		// 			return
// 		// 		}
// 		// 		args, reply := rf.GetReplicateArgsAndReply(server, canFinish)
// 		// 		rf.mu.Unlock()
// 		// 		if ok := rf.sendAppendEntries(server, &args, &reply); ok {
// 		// 			rf.mu.Lock()
// 		// 			if reply.Term > rf.currentTerm { // 它照样起到heartbeat作用
// 		// 				rf.ChangeState(Follower, true)
// 		// 				rf.mu.Unlock()
// 		// 				return
// 		// 			} else { // 检测是否成功如果成功就可以发送后面的Entries了
// 		// 				if reply.Success {
// 		// 					if canFinish {
// 		// 						haveSendAllCMD = true
// 		// 					}
// 		// 					canFinish = true
// 		// 					rf.nextIndex[server] = MaxInt(rf.nextIndex[server], args.PrevLogIndex+len(args.Entries)+1)
// 		// 				} else {
// 		// 					rf.nextIndex[server]--
// 		// 				}
// 		// 			}
// 		// 			rf.mu.Unlock()
// 		// 		} else { // 如果掉线 类似二进制指数退让法 让他不要频繁的rpc

// 		// 			backOffTime = rf.BackOff(backOffTime) // retry 的时候可能Leader已经寄了，说明agree还是得定时做一下

// 		// 		}
// 		// 	}
// 		// 	callback()
// 		// }(server)
// 	}
// }

// func (rf *Raft) CheckIndex() {
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()

// 	for server := 0; server < len(rf.peers); server++ {
// 		if server == rf.me {
// 			continue
// 		}
// 		if rf.nextIndex[server] < rf.commitIndex+1 {

// 			go rf.SendReplicate(server, func() {}, 0)
// 		}
// 	}
// }

// func (rf *Raft) PeriodicallyCheckIndexTicker() {
// 	for rf.killed() == false {
// 		time.Sleep(time.Duration(randSleepTime(rf.me)) * time.Millisecond)
// 		rf.mu.Lock()
// 		if rf.state == Leader {
// 			rf.CheckIndex()
// 		}
// 		rf.mu.Unlock()
// 	}
// }

// func (rf *Raft) CommandAgreementTicker() {
// 	for rf.killed() == false {
// 		time.Sleep(time.Duration(randSleepTime(rf.me)) * time.Millisecond)
// 		rf.mu.Lock()
// 		if rf.state == Leader {
// 			if len(rf.log) > 0 && rf.nextIndex[rf.me] <= rf.log[len(rf.log)-1].CommandIndex {
// 				rf.mu.Unlock()
// 				rf.ReachAggreement()
// 			} else {
// 				rf.mu.Unlock()
// 				rf.CheckIndex()
// 			}
// 		} else {
// 			rf.mu.Unlock()
// 		}
// 	}
// }
