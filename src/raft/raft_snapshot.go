package raft

import (
	"bytes"
	"time"

	"6.824/labgob"
)

// type AppendEntriesArgs struct {
// 	Term         int
// 	LeaderId     int
// 	PrevLogIndex int
// 	PrevLogTerm  int
// 	Entries      []ApplyMsg
// 	LeaderCommit int
// }

// type AppendEntriesReply struct {
// 	Term     int
// 	Success  bool
// 	Conflict bool
// 	XTerm    int
// 	XIndex   int
// 	XLen     int
// }

// func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
// 	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
// 	return ok
// }

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
	Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) oldLogs() []ApplyMsg {
	var msgs []ApplyMsg
	r := bytes.NewBuffer(rf.persister.ReadSnapshot())
	d := labgob.NewDecoder(r)
	d.Decode(&msgs)
	return msgs
}

// func (rf *Raft) getSnapshot(index int) []byte {
// 	logs := rf.oldLogs()
// 	// for idx, entry := range rf.log {
// 	// 	if entry.CommandIndex > index {
// 	// 		break
// 	// 	}
// 	// 	logs = append(logs, entry)
// 	// }
// 	w := bytes.NewBuffer(buf)
// 	e := labgob.NewEncoder(w)
// 	// 应该encode哪些变量，直接encode log

// 	return buf.Bytes()
// }

// func (rf *Raft) readSnapshot(data []byte) {

// 	r := bytes.NewBuffer(data)
// 	d := labgob.NewDecoder(r)
// 	// 解码

// }

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// follower需要判断是否安装snapshot
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	// snapshot := make([]ApplyMsg, 0) // snapshot不是log
	// r := bytes.NewBuffer(args.Data)
	// d := labgob.NewDecoder(r)
	// d.Decode(&snapshot)
	// if len(snapshot) == 0 {
	// 	return
	// }
	if args.LastIncludedIndex == 0 {
		return
	}

	snapshotApplyMsg := ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}

	go func() {
		rf.applyCh <- snapshotApplyMsg
	}()
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2D).
	// 判断是否要接受leader传来的snapshot
	if lastIncludedIndex <= rf.lastIncludingIndex {
		return false
	}

	tempLog := []ApplyMsg{}
	for i := lastIncludedIndex + 1; i <= rf.getLastLogIndex(); i++ {
		tempLog = append(tempLog, *rf.logAt(i))
	}

	// will install log
	DPrintf("Sever %v will install log at %v", rf.me, lastIncludedIndex)

	// page12 rule 1, if contain new log, discard self log and use the entire snapshot
	if lastIncludedIndex > rf.getLastLogIndex() {
		// 全部接受
		rf.log = []ApplyMsg{}
		rf.commitIndex = MaxInt(rf.commitIndex, lastIncludedIndex)
		rf.lastApplied = MaxInt(rf.lastApplied, lastIncludedIndex)
		rf.lastIncludingIndex = lastIncludedIndex
		rf.lastIncludingTerm = lastIncludedTerm
		rf.persist()
		return true
	}

	// rule2 如果都是老东西，说明这些老东西需要进垃圾桶了
	rf.log = tempLog

	rf.persist()
	return false
}

func (rf *Raft) sendSnapshotRountine() {
	for server := 0; server < len(rf.peers); server++ {
		// 开几个协程分别发送rpc
		if server == rf.me {
			continue
		}
		curServer := server
		go func() {
			rf.mu.Lock()
			args := InstallSnapshotArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.lastIncludingIndex,
				LastIncludedTerm:  rf.lastIncludingTerm,
				Data:              rf.persister.ReadSnapshot(),
				Done:              false,
			}
			reply := InstallSnapshotReply{}
			rf.mu.Unlock()

			ok := rf.sendInstallSnapshot(curServer, &args, &reply)
			if !ok {
				return
			}

			rf.mu.Lock()
			// check rpc timeout
			if rf.currentTerm != args.Term || rf.state != Leader {
				rf.mu.Unlock()
				return
			}
			if reply.Term > rf.currentTerm {
				rf.state = Follower
				rf.votedFor = -1
				rf.persist()
				rf.resetElectionTimer()
				rf.mu.Unlock()
				return
			}
			// 发送snapshot可以替代appendEntries快速更新那个数据结构
			rf.nextIndex[curServer] = MaxInt(args.LastIncludedIndex+1, rf.nextIndex[curServer])
			rf.matchIndex[curServer] = MaxInt(args.LastIncludedIndex, rf.matchIndex[curServer])

			rf.mu.Unlock()
		}()
	}
}

func (rf *Raft) SnapshotTicker() {
	for !rf.killed() {
		time.Sleep(time.Duration(1000) * time.Millisecond)
		rf.mu.Lock()
		if rf.state == Leader {
			rf.sendSnapshotRountine() // 应该就是像applier一样的搞个条件变量
		}
		rf.mu.Unlock()
	}
}
