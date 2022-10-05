package raft

import (
	"log"
	"sync"
)

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
	VoteFor     int
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	DPrintf("[ELECT CLIENT] Server%v@Term%v receive the request vote from Server%v@Term%v\n", rf.me, rf.currentTerm, args.CandidateID, args.Term)
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
	} else if args.Term == rf.currentTerm {
		// 每个term一个server只能投票一次，投给谁之后不能改
		if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
			// log.Printf("(Server %v, Term %v) Vote for (Server %v, Term %v)\n", rf.me, rf.currentTerm, args.CandidateID, args.Term)
			reply.VoteGranted = true
			reply.Term = rf.currentTerm
			rf.mu.Unlock()
			rf.ChangeState(Follower)
			rf.mu.Lock()
			rf.votedFor = args.CandidateID
		} else {
			reply.VoteGranted = false
			reply.Term = rf.currentTerm
		}
	} else if args.Term > rf.currentTerm {
		// 如果term比较大，那就任它当老大了
		// log.Printf("(Server %v, Term %v) Vote for (Server %v, Term %v)\n", rf.me, rf.currentTerm, args.CandidateID, args.Term)
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		rf.ChangeState(Follower)
		rf.mu.Lock()
		rf.votedFor = args.CandidateID
		rf.currentTerm = args.Term
	}
	if reply.VoteGranted{
		DPrintf("[ELECT CLIENT] Server%v@Term%v vote for Server%v@Term%v\n", rf.me, rf.currentTerm, args.CandidateID, args.Term)
	} else {
		DPrintf("[ELECT CLIENT] Server%v@Term%v vote for Server%v@Term%v\n", rf.me, rf.currentTerm, args.CandidateID, args.Term)
	}
	
	rf.mu.Unlock()
}



func (rf *Raft) CollectTicket() bool {
	start := getCurrentTime()
	ticket := 1
	ticketCnt := make([][]int, len(rf.peers))
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	rf.mu.Unlock()
	DPrintf("[ELECT SERVER] Server %v@Term %v running an election\n", rf.me, rf.currentTerm)

	var wg sync.WaitGroup

	for server, _ := range rf.peers {
		if server == rf.me {
			continue
		}
		// parallay request
		wg.Add(1)
		go func(server int) {
			defer wg.Done()
			args := RequestVoteArgs{
				Term:        currentTerm,
				CandidateID: rf.me,
			}
			reply := RequestVoteReply{}
			if ok := rf.sendRequestVote(server, &args, &reply); ok {
				if reply.VoteGranted {
					ticketCnt[server] = []int{server, reply.Term, 1}
					DPrintf("[ELECT SERVER] Server%v@Term%v vote for Server%v@Term%v\n", server, reply.Term, rf.me, rf.currentTerm)
				} else {
					ticketCnt[server] = []int{server, reply.Term, 0}
					DPrintf("[ELECT SERVER] Server%v@Term%v reject vote for Server%v@Term%v\n", server, reply.Term, rf.me, rf.currentTerm)
				}
			} else {
				ticketCnt[server] = []int{server, 0, 0}
			}
		}(server)
	}
	wg.Wait()
	DPrintf("[ELECT SERVER] Server%v@Term%v earn tickets%v\n", rf.me, rf.currentTerm, ticketCnt)
	for server, _ := range rf.peers {
		if server == rf.me {
			continue
		}
		if ticketCnt[server][2] == 1 {
			ticket++
		}
	}
	end := getCurrentTime()
	DPrintf("Collect ticket cost %vms\n", end-start)
	return ticket > len(rf.peers)/2
}

func (rf *Raft) ChangeState(state int) {
	rf.mu.Lock()
	stateName := []string {"FOLLOWER","CANDADITE","LEADER"}
	DPrintf("[STATE] Server %v State change FROM %v to %v", rf.me, stateName[rf.state], stateName[state])
	if state == Follower {
		rf.candidating = false
		rf.state = Follower
	} else if state == Candidate {
		rf.currentTerm++
		rf.votedFor = rf.me
		rf.state = Candidate
		rf.candidating = true
	} else {
		rf.votedFor = rf.me
		rf.state = Leader
	}
	rf.mu.Unlock()
}

func (rf *Raft) ElectionRoutine() bool {
	// if rf.state == Follower {
	// 	rf.ChangeState(Candidate)
	// 	log.Printf("[INFO] Server %v, Term %v change to candidate\n", rf.me, rf.currentTerm)
	// } else if rf.state == Candidate {
	// 	log.Printf("[FAIL] Server %v, Term %v Election Clash", rf.me, rf.currentTerm)
	// } else {
	// 	log.Printf("[Error] Server %v, Term %v Leader can not change to candidate\n", rf.me, rf.currentTerm)
	// 	return false
	// }
	rf.ChangeState(Candidate)
	// log.Printf("(Server %v, Term %v) earn %v tickets of all %v\n", rf.me, rf.currentTerm, ticket, allTicket)
	var ok bool
	if ok = rf.CollectTicket(); ok {
		rf.ChangeState(Leader)
		rf.LeaderRoutine()
		log.Printf("==>(Server %v, Term%v) is select as leader\n", rf.me, rf.currentTerm)
	} else {
		rf.ChangeState(Follower)
	}
	return ok
}
