package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const (
	Follower  = 0
	Candidate = 1
	Leader    = 2
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//2a:
	status      int
	currentTerm int
	votedFor    int
	voteMap     map[int]bool
	heartBeat   time.Time
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	iterm := rf.currentTerm
	isleader := rf.status == Leader
	// Your code here (2A).
	return iterm, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term     int
	LeaderId int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type AppendEntriesReply struct {
	// Your data here (2A).
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm > args.Term {
		DPrintf("AppendEntries: server%d get from%d term:%d but i term is %d", rf.me, args.LeaderId, args.Term, rf.currentTerm)
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	rf.currentTerm = args.Term
	if rf.status == Leader {
		rf.status = Follower
		rf.votedFor = -1
	}
	rf.SetHeartbeat()
	reply.Success = true
	reply.Term = rf.currentTerm
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateId int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.SetHeartbeat()
	DPrintf("Requote: server%d get from%d term:%d", rf.me, args.CandidateId, args.Term)
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if exist, _ := rf.voteMap[args.Term]; exist {
		DPrintf("Requote: server%d not Granted1 from%d term:%d", rf.me, args.CandidateId, args.Term)
		return
	}
	if args.Term < rf.currentTerm {
		DPrintf("Requote: server%d not Granted2 from%d term:%d", rf.me, args.CandidateId, args.Term)
		return
	}
	DPrintf("Requote: server%d Granted from%d term:%d", rf.me, args.CandidateId, args.Term)
	reply.VoteGranted = true
	rf.voteMap[args.Term] = true
	rf.votedFor = args.CandidateId
	return
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.

// 使多个rpc并发进行，并使用chan来统计投票结果，结果只有两种（票数大于一半和小于一半）
func (rf *Raft) sendRequestVote(c chan bool, server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	DPrintf("I KILL ONE")
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		//将rf状态用作条件得先加锁取出条件，好麻烦
		rf.mu.Lock()
		isLeader := rf.status
		rf.mu.Unlock()
		if isLeader == Leader {
			rf.mu.Lock()
			args := AppendEntriesArgs{
				Term:     rf.currentTerm,
				LeaderId: rf.me,
			}
			rf.mu.Unlock()
			//	var wait1 sync.WaitGroup
			for server := range rf.peers {
				if server == rf.me {
					continue
				}
				//		wait1.Add(1)
				//以下是使用协程并发发出,需要使用waitgroup
				go func(num int) {
					reply := AppendEntriesReply{}
					ok := rf.peers[num].Call("Raft.AppendEntries", &args, &reply)

					if ok == false {
						DPrintf("AppendEntriesRPC: server%d send to%d get false", rf.me, num)
					} else {
						if reply.Success == false {
							rf.mu.Lock()
							DPrintf("AppendEntries: server%d send to%d get false return term%d", rf.me, num, reply.Term)
							rf.status = Follower
							rf.currentTerm = reply.Term
							rf.SetHeartbeat()
							rf.mu.Unlock()
						}
					}
					//			wait1.Done()
				}(server)

				//TODO 使用waitgroup
				//reply := AppendEntriesReply{}
				//ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
				//if ok == false {
				//	DPrintf("AppendEntries: server%d send to%d get false return term%d", rf.me, server, reply.Term)
				//}
			}
			//	wait1.Wait()
			RandSleep(100, 10)
			continue
		}
		rf.mu.Lock()
		istovote := time.Now().After(rf.heartBeat)
		rf.mu.Unlock()
		if istovote {
			//TODO 开始选举
			DPrintf("Requote: server%d start to vote", rf.me)
			rf.mu.Lock()
			rf.votedFor = rf.me
			args := RequestVoteArgs{}
			rf.currentTerm++

			//记录已经投票过了
			rf.voteMap[rf.currentTerm] = true
			args.Term = rf.currentTerm
			args.CandidateId = rf.me
			count := 0
			length := len(rf.peers)
			count++
			rf.mu.Unlock()

			//也许我应该为每一个rpc产生一个协程，但是我如何处理返回值，或者是等待返回
			voted := make(chan bool, length-1)
			var wait sync.WaitGroup
			timeout := time.After(100 * time.Millisecond)
			go func() {
				for server := range rf.peers {
					if server == rf.me {
						continue
					}
					wait.Add(1)
					go func(num int) {
						reply := RequestVoteReply{}
						ok := rf.peers[num].Call("Raft.RequestVote", &args, &reply)
						if ok == false {
							DPrintf("RequoteRPC: server%d send to%d get false", rf.me, num)
							voted <- false
						} else {
							if reply.VoteGranted == true {
								voted <- true
							} else {
								voted <- false
							}
						}
						wait.Done()
					}(server)
				}
				wait.Wait()
				close(voted)
			}()

			for {
				select {
				case v, ok := <-voted:
					if !ok {
						// All votes received
						goto VoteCountingDone
					}
					if v == true {
						rf.mu.Lock()
						count++
						rf.mu.Unlock()
					}
				case <-timeout:
					DPrintf("RequoteRPC: Timeout waiting for votes")
					goto VoteCountingDone
				}
			}
			//采用for循环，但是无法处理超时所以使用select来监听超时
			//for v := range voted {
			//	if v == true {
			//		count++
			//	}
			//}

			//以下是非并发请求
			//for server := range rf.peers {
			//	if server == rf.me {
			//		continue
			//	}
			//	reply := RequestVoteReply{}
			//
			//	ok := rf.peers[server].Call("Raft.RequestVote", &args, &reply)
			//	if ok == false {
			//		DPrintf("Requote: server%d send to%d get false", rf.me, server)
			//	}
			//	if reply.VoteGranted == true {
			//		count++
			//	}
			//}
		VoteCountingDone:
			rf.mu.Lock()
			if count > length/2 {

				DPrintf("Requote: server%d is Leader", rf.me)
				rf.status = Leader
				rf.mu.Unlock()
				continue
			} else {
				DPrintf("Requote: server%d is  Follower", rf.me)
				rf.status = Follower
				rf.votedFor = -1
			}
			rf.mu.Unlock()
		}

		RandSleep(150, 50)
	}
}
func RandSleep(atLeast int, flow int) {
	rand.Seed(time.Now().UnixNano())
	time.Sleep(time.Duration(rand.Intn(flow)+atLeast) * time.Millisecond)
}
func (rf *Raft) SetHeartbeat() {
	rf.heartBeat = time.Now().Add(100 * time.Millisecond)
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.mu.Lock()
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	rf.voteMap = make(map[int]bool)
	rf.status = Follower
	rf.votedFor = -1
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.heartBeat = time.Now()

	// start ticker goroutine to start elections
	rf.SetHeartbeat()
	rf.mu.Unlock()
	go rf.ticker()
	return rf
}
