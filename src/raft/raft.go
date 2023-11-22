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
type Log struct {
	Command interface{}
	Term    int
}

// A Go object implementing a single Raft peer.
const (
	Follower  = 0
	Candidate = 1
	Leader    = 2
)

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state        int
	currentTerm  int
	votedFor     int
	log          []Log
	commitIndex  int
	lastApplied  int
	nextIndex    []int
	matchIndex   []int
	heartBeat    bool
	lastLogIndex int
	lastLogTerm  int
	applyCh      chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A).
	term = rf.currentTerm
	if rf.state == Leader {
		isleader = true
	}
	return term, isleader
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

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// 根据paper，1.比较Term 2.日志至少和当前一样新
//
// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// 过时的Term直接拒绝
	if rf.currentTerm > args.Term {
		reply.VoteGranted = false
		return
	}
	// 与当前Term相等的情况
	if rf.currentTerm == args.Term {
		// 本地最后一条Log小，没投过就投
		if rf.lastLogTerm < args.LastLogTerm || (rf.lastLogTerm == args.LastLogTerm && rf.lastLogIndex <= args.LastLogIndex) {
			if rf.votedFor == -1 {
				reply.VoteGranted = true
				DPrintf("VoteGranted1 server:%d args: %d myterm %d", rf.me, args.Term, rf.currentTerm)
				rf.votedFor = args.CandidateId
				rf.heartBeat = true
				return
			}
		}
	}
	// 当前Term小于收到的Term，投票条件判断
	if rf.lastLogTerm < args.LastLogTerm || (rf.lastLogTerm == args.LastLogTerm && rf.lastLogIndex <= args.LastLogIndex) {
		reply.VoteGranted = true
		DPrintf("VoteGranted2  server:%d  args: %d myterm %d", rf.me, args.Term, rf.currentTerm)
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
		rf.heartBeat = true
		return
	}
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PreLogTerm   int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	//拒绝一个过时的请求
	if rf.currentTerm > args.Term {
		reply.Success = false
	} else {
		rf.currentTerm = args.Term
		rf.state = Follower
		reply.Success = true
		rf.heartBeat = true
	}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := rf.currentTerm
	isLeader := false
	if rf.state != Leader {
		return index, term, isLeader
	}

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
		rf.mu.Lock()
		rf.heartBeat = false
		rf.mu.Unlock()
		randomSleep(200, 50)
		rf.mu.Lock()
		if rf.state == Follower {
			if rf.heartBeat == false {
				//开始选举，但是选举后要检查自己状态是否改变 赢了，没赢
				//要做超时处理
				rf.currentTerm++
				rf.state = Candidate
				args := RequestVoteArgs{}
				args.Term = rf.currentTerm
				args.CandidateId = rf.me
				args.LastLogIndex = rf.lastLogIndex
				args.LastLogTerm = rf.lastLogTerm
				rf.mu.Unlock()
				timeout := time.After(30 * time.Millisecond)
				channel := make(chan bool)
				waitgroup := sync.WaitGroup{}
				for server := range rf.peers {
					if server == rf.me {
						continue
					}
					waitgroup.Add(1)
					go func(id int) {
						reply := RequestVoteReply{}
						ret := rf.peers[id].Call("Raft.RequestVote", &args, &reply)
						if ret == false {
							DPrintf("RequestVoteRPC failed server %d send to %d", rf.me, id)
							channel <- false
						} else {
							if reply.VoteGranted == true {
								channel <- true
							} else {
								channel <- false
							}
						}
						waitgroup.Done()
					}(server)
				}
				go func() {
					waitgroup.Wait()
					close(channel)
				}()
				end := false
				count := 1
				for {
					if end == true {
						break
					}
					select {
					case v, ok := <-channel:
						if !ok {
							end = true
							break
						}
						if v {
							count++
						}
					case <-timeout:
						end = true
					}
				}
				rf.mu.Lock()
				//选举结果是否有效，有没有在选举期间变成Follower
				if rf.state == Follower {
					rf.mu.Unlock()
					continue
				} else {
					if count > len(rf.peers)/2 {
						rf.state = Leader
						DPrintf("server %d win the vote rf.SendAppendEntries", rf.me)
						//定时发送心跳给其他
						rf.mu.Unlock()
						go rf.SendAppendEntries()
					} else {
						DPrintf("server %d 选举失败", rf.me)
						rf.state = Follower
						rf.mu.Unlock()
					}
				}
			} else {
				DPrintf("server %d heartbeat true", rf.me)
				rf.mu.Unlock()
			}
		} else {
			rf.mu.Unlock()
		}

	}
}

// 这个函数通常被协程开启，用于间歇性发送心跳，如果察觉到自己被Kill了的就退出或者自己不是Leader了
func (rf *Raft) SendAppendEntries() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.state != Leader {
			DPrintf("server %d is not Leader", rf.me)
			rf.mu.Unlock()
			break
		} else {
			DPrintf("server %d is Leader", rf.me)
			args := AppendEntriesArgs{}
			args.Term = rf.currentTerm
			args.LeaderId = rf.me
			args.Entries = nil
			rf.mu.Unlock()
			for server := range rf.peers {
				if rf.me == server {
					continue
				}
				reply := AppendEntriesReply{}
				go func(id int) {
					ret := rf.peers[id].Call("Raft.AppendEntries", &args, &reply)
					if ret == false {
						DPrintf("AERPCerr:server %d send to %d", rf.me, id)
					} else {
						if reply.Success == false {
							DPrintf("AEerr:server %d is overtime", rf.me)
							rf.mu.Lock()
							defer rf.mu.Unlock()
							rf.state = Follower
							rf.currentTerm = reply.Term
							rf.votedFor = -1
							rf.setHeatBeat()
						} else {
							DPrintf("AE:server %d send to %d", rf.me, id)
						}
					}
				}(server)
			}

		}
		time.Sleep(100 * time.Millisecond)
	}
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
func (rf *Raft) setHeatBeat() {
	rf.heartBeat = true
}
func randomSleep(atleast int, flow int) {
	time.Sleep(time.Duration(atleast+rand.Int()%flow) * time.Millisecond)
}
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// Your initialization code here (2A, 2B, 2C).
	rf.votedFor = -1
	rf.applyCh = applyCh
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.heartBeat = false
	rf.log = make([]Log, 0, 0)
	rf.lastLogTerm = 0
	rf.lastLogIndex = 0
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
