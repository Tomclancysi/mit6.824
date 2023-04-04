package raft

import "fmt"

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []ApplyMsg
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term     int
	Success  bool
	Conflict bool
	XTerm    int
	XIndex   int
	XLen     int
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) PrintCurState() {
	stack := ""
	for i := 1; i <= len(rf.log); i++ {
		if rf.commitIndex >= rf.log[i-1].CommandIndex {
			// stack += fmt.Sprintf("[%v]", rf.log[i-1].CommandIndex)
			stack += fmt.Sprintf("[%v,%v]", rf.log[i-1].Command, rf.log[i-1].CommandTerm)
		} else {
			stack += fmt.Sprintf(" %v,%v ", rf.log[i-1].Command, rf.log[i-1].CommandTerm)
		}
	}
	stack += fmt.Sprintf("|<%v, %v>", rf.commitIndex, rf.lastApplied)
	if rf.state == Leader {
		DPrintf("T%v!!!!!!!!!Server%v : %v", rf.currentTerm, rf.me, stack)
	} else {
		DPrintf("T%v>>>>>>>>>Server%v : %v", rf.currentTerm, rf.me, stack)
	}

}

func (rf *Raft) GetReplicateArgsAndReply(server int, appendCMD bool) (AppendEntriesArgs, AppendEntriesReply) {
	// 当发送一次rpc，应该发给它什么？
	// 1. PrevLogIndex，PrevLogTerm。 leader需要通过rpc的方式，自动确认客户端的log到底在哪
	//    这就涉及到leader和follower battle的过程。也就是和nextIndex和matchIndex battle。
	//       其中nextIndex表示服务器认为的follower的下一个log。**matchIndex**是已知的
	//       复制到follower上面去的index
	// 2. 琐碎的Leader当前状态，当前确认commitIndex
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex,
	}

	// impl1 ： nextIndex始终大于等于1，我需要验证它的上一条能否对齐
	if rf.nextIndex[server]-1 > 0 { // 判断这条记录是否对应啊
		args.PrevLogIndex = rf.nextIndex[server] - 1
		args.PrevLogTerm = rf.getLogTerm(args.PrevLogIndex) // BUGPOINT rf.logAt(args.PrevLogIndex).CommandTerm //rf.log[args.PrevLogIndex-1].CommandTerm
		args.Entries = rf.truncateAfter(args.PrevLogIndex)  // rf.log[args.PrevLogIndex:] // 如果上一个index确认，发送这后面的即可
	} else {
		// 不需要验证
		args.PrevLogIndex = 0
		args.PrevLogTerm = 0
		args.Entries = rf.log
	}

	// ！todo暂时放弃优化 优化什么时候把log发过去，难道每次rpc都发吗，但是回滚nextIndex是个O（n）的过程

	reply := AppendEntriesReply{}
	return args, reply
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 1.老掉牙了，说明有的地方准备投票新的了 那我也没必要heatbeat了
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return // bugpoint 这竟然没有返回
	}
	if rf.currentTerm < args.Term { // 如果他是新的term，自然要跟随了，投它
		rf.currentTerm = args.Term
	}
	// 返回值填好了
	reply.Success = true
	reply.Term = rf.currentTerm

	// battle，如果
	rf.state = Follower
	rf.votedFor = args.LeaderId
	rf.resetElectionTimer()

	// 2. 和Leader确认当前PrevIndex判断指定位置的Term Index相同返回true
	if args.PrevLogIndex > 0 {
		// 需要确认,如果该位置没有值或者有值且不一样，删除该位置和它之后的log
		if args.PrevLogIndex > rf.getLastLogIndex() ||
			(args.PrevLogIndex > rf.lastIncludingIndex && args.PrevLogIndex <= rf.getLastLogIndex() && rf.logAt(args.PrevLogIndex).CommandTerm != args.PrevLogTerm) { // this term is not commandterm. eeeeeee
			reply.Success = false
			reply.Conflict = true
			if args.PrevLogIndex > rf.lastIncludingIndex && args.PrevLogIndex <= rf.getLastLogIndex() {
				xterm := rf.logAt(args.PrevLogIndex).CommandTerm
				xindex := 0 // 2D todo 如果要支持snapshot，这里应该怎么改？
				for i := args.PrevLogIndex - 1; i > 0 && i > rf.lastIncludingIndex; i-- {
					if rf.logAt(i).CommandTerm != xterm {
						xindex = i
						break
					}
				}
				reply.XIndex = xindex
				reply.XTerm = xterm
				reply.XLen = len(rf.log) // 2D todo
				// 删除掉不agree的部分
				rf.log = rf.truncateIncBefore(args.PrevLogIndex - 1) // rf.log[:args.PrevLogIndex-1]
				rf.persist()
			} else {
				reply.XIndex = -1
				reply.XTerm = -1                  // xterm 为-1表示不予置评
				reply.XLen = rf.getLastLogIndex() // 2D todo
			}
			return
		}

		if args.PrevLogIndex > rf.lastIncludingIndex && args.PrevLogIndex <= rf.getLastLogIndex() && rf.logAt(args.PrevLogIndex).CommandTerm == args.PrevLogTerm {
			rf.log = rf.truncateIncBefore(args.PrevLogIndex)
		}
	}

	// 想一想这还用确认吗？对的，不需要！！无脑往自己的log里面放就完事了
	// BUGPOINT 这里要怎么放？？应该从确认的那条记录开始往后放，也就是说《之前如果确认点后面有值那就得清空》
	wantedIndex := rf.getLastLogIndex() + 1
	for _, cmd := range args.Entries {
		if cmd.CommandIndex == wantedIndex {
			wantedIndex++
			rf.log = append(rf.log, cmd)
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		if len(rf.log) == 0 {
			rf.persist()
			return
		}

		// BugPoint更新commitID之后是不是周期性的提交一下任务？？ 但是应该搞一个单独的协程
		// 为什么commit的index  log里面都没有？
		rf.commitIndex = MinInt(rf.getLastLogIndex(), args.LeaderCommit)
		rf.apply()
	}
	rf.persist()
	// DPrintf("(AppendEntries)[%d,%d] %v, commitIndex=%v", rf.me, rf.currentTerm, rf.log, rf.commitIndex)
}

func (rf *Raft) LeaderRoutine() {
	// 两个任务
	// 1. 心跳 2. 修正log 3. 为新command投票(隐式的)
	for server, _ := range rf.peers {
		if server == rf.me {
			continue
		}
		// leader每个周期要把这些信息都发给peer，通过心跳的机制发送那个
		go func(server int) {
			rf.mu.Lock()
			if rf.state != Leader {
				rf.mu.Unlock()
				return
			}
			// 准备发送rpc，2D引入了snapshot如果要确认的都进了垃圾桶了，就不要appendentries了。但是应该发送snapshot
			if rf.lastIncludingIndex > 0 && rf.lastIncludingIndex > rf.nextIndex[server]-1 {
				// 如果要验证的已经snapshot了，这tm就死锁了。 2D这里应该是小于
				// 可以像applier一样把snapshot协程阻塞住，然后此时让他发送一次
				rf.sendSnapshotToServer(server)
				rf.mu.Unlock()
				return
			}
			args, reply := rf.GetReplicateArgsAndReply(server, false)
			rf.mu.Unlock()

			if ok := rf.sendAppendEntries(server, &args, &reply); ok {
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.state = Follower
					rf.votedFor = -1
					rf.persist()
					rf.resetElectionTimer()
				} else {
					if args.Term != rf.currentTerm {
						rf.mu.Unlock()
						return
					}
					// 检测是否battle是否成功如果成功就可以发送后面的Entries了
					if reply.Success {
						// 成功说明我们发的他都认了,注意不要直接修改值，而是要max一下
						rf.nextIndex[server] = MaxInt(rf.nextIndex[server], args.PrevLogIndex+len(args.Entries)+1)
						rf.matchIndex[server] = MaxInt(rf.matchIndex[server], rf.nextIndex[server]-1)
						// 检查它是否承认新的Command，这里由于我们每次都发送了全量的log，因此他肯定会收到这个log，但是commit倒没有
						// 注意：其实这里不需要一直请求直到半数以上的满足要求，而是通过检查log的方式
					} else {
						// MaybeBugPoint以非常慢的速度下降，同样警惕失效rpc，如果失效了会怎么样
						// 如何使用xterm优化
						if args.PrevLogIndex == rf.nextIndex[server]-1 {
							if reply.Conflict {
								xterm := reply.XTerm
								xindex := reply.XIndex
								if xterm < 0 {
									rf.nextIndex[server] = reply.XLen // 直接检查最后一条
								} else {
									// 由于引入了snapshot，不要在意过往了
									if args.PrevLogIndex <= rf.lastIncludingIndex {
										rf.mu.Unlock()
										return
									}
									curIndex := args.PrevLogIndex - 1
									for ; curIndex > xindex && curIndex > rf.lastIncludingIndex; curIndex-- {
										if rf.getLogTerm(curIndex) == xterm {
											break
										}
									}
									rf.nextIndex[server] = curIndex + 1
								}
							} else {
								rf.nextIndex[server] = args.PrevLogIndex // 太慢了
							}
						}
					}
				}
				rf.mu.Unlock()
			}
		}(server)
	}

	// 3. 确认新事务
	// 从最大的index开始往前遍历
	for i := rf.commitIndex + 1; i <= rf.getLastLogIndex(); i++ {
		matchCount := 1
		for j := range rf.peers {
			if j == rf.me {
				continue
			}
			if rf.matchIndex[j] >= i && rf.getLogTerm(i) == rf.currentTerm {
				// 注意值commit 当前Term里面的command
				matchCount++
			}
		}
		if matchCount*2 > len(rf.peers) {
			rf.commitIndex = i
			rf.apply()
		}
	}

	// nextCommitIndex := rf.commitIndex + 1 // bugpoint persist竟然不需要commitIndex这是因为有了也没用，不能根据这个判断
	// // 那已经applied了之后的，如何避免再次applied？？
	// alreadyInLog := 1
	// for server, _ := range rf.peers {
	// 	if server == rf.me {
	// 		continue
	// 	}
	// 	if rf.matchIndex[server] >= nextCommitIndex {
	// 		alreadyInLog++
	// 	}
	// }
	// if alreadyInLog > int(len(rf.peers)/2) {
	// 	rf.commitIndex = nextCommitIndex
	// 	// DPrintf("(LeaderRoutine)[%d,%d]curCommitIndex=%v", rf.me, rf.currentTerm, rf.commitIndex)
	// }
}
