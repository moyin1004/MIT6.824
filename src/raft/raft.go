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
	//	"bytes"

	"bytes"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
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
	state                State
	Data                 []byte
	LeaderLastActiveTime int64
	// Leader
	LeaderData *LeaderData `json:"LeaderData,omitempty"`
	ServerData *ServerData
	// other
	r         *rand.Rand
	applyCh   chan ApplyMsg
	applyCond *sync.Cond // used to wakeup applier goroutine after committing new entries
}

type LeaderData struct {
	NextIndex  []int `json:"NextIndex"`
	MatchIndex []int `json:"MatchIndex"` // 针对所有的服务器，内容是已知被复制到每个服务器上的最高日志条目号
}

type ServerData struct {
	CommitIndex int `json:"CommitIndex"`
	LastApplied int `json:"LastApplied"`
}

type Log struct {
	Term    int         `json:"Term"`
	Index   int         `json:"Index"`
	Command interface{} `json:"Command"`
}

type State struct {
	CurrentTerm int   `json:"CurrentTerm"`
	VotedFor    int   `json:"VotedFor"`
	Logs        []Log `json:"Logs"` // 第一条占位 不是实际日志
}

func (rf *Raft) tranFollower(term int) bool {
	if term > rf.state.CurrentTerm {
		rf.state.CurrentTerm = term
		rf.state.VotedFor = -1
		rf.LeaderData = nil
		DPrintf("tranFollower me:%d, resume follower", rf.me)
		return true
	}
	return false
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.state.CurrentTerm
	isleader = rf.LeaderData != nil
	rf.mu.Unlock()
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(rf.state)
	if err != nil {
		DPrintf("persist failed, err:%v", err)
		return
	}
	// 这里直接使用json序列化state时 command反序列化时是float类型 会不匹配测试的int
	// 使用实验提供的工具
	// rf.persister.SaveRaftState(w.Bytes())
	rf.persister.SaveStateAndSnapshot(w.Bytes(), rf.Data)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var state State
	err := d.Decode(&state)
	if err == nil {
		rf.state = state
	}
	DPrintf("readPersist me:%d, state:%v, err:%v", rf.me, rf.state, err)
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if lastIncludedIndex <= rf.ServerData.CommitIndex {
		return false
	}
	if lastIncludedIndex > rf.state.Logs[len(rf.state.Logs)-1].Index {
		rf.state.Logs = []Log{{
			Term:    lastIncludedTerm,
			Index:   lastIncludedIndex,
			Command: nil,
		}}
	} else {
		tmp := rf.state.Logs[:]
		rf.state.Logs = []Log{{
			Term:    lastIncludedTerm,
			Index:   lastIncludedIndex,
			Command: nil,
		}}
		for i := lastIncludedIndex - tmp[0].Index + 1; i < len(tmp); i++ {
			rf.state.Logs = append(rf.state.Logs, tmp[i])
		}
	}
	rf.Data = snapshot
	rf.ServerData.LastApplied, rf.ServerData.CommitIndex = lastIncludedIndex, lastIncludedIndex
	rf.persist()
	DPrintf("CondInstallSnapshot me:%d, state:%v, commintIndex:%d", rf.me, rf.state, rf.ServerData.CommitIndex)
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	firstIndex := rf.state.Logs[0].Index
	if index <= firstIndex {
		DPrintf("Snapshot, cur:%d, target:%d", firstIndex, index)
		return
	}
	tmp := rf.state.Logs[:]
	// 已压缩的最后一条日志
	rf.state.Logs = []Log{{
		Term:    rf.state.Logs[index-firstIndex].Term,
		Index:   index,
		Command: nil,
	}}
	DPrintf("Snapshot, log num:%d", len(tmp))
	// index+1开始
	for i := index - firstIndex + 1; i < len(tmp); i++ {
		rf.state.Logs = append(rf.state.Logs, tmp[i])
	}
	rf.Data = snapshot
	rf.persist()
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
	CurrentTerm int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if args.Term < rf.state.CurrentTerm {
		reply.CurrentTerm = rf.state.CurrentTerm
		return
	}
	DPrintf("vote req:%v, me:%d, cur:%d", args, rf.me, rf.state.CurrentTerm)
	rf.tranFollower(args.Term)
	reply.CurrentTerm = rf.state.CurrentTerm
	lastLog := &rf.state.Logs[len(rf.state.Logs)-1]
	// 候选者日志更旧
	if args.LastLogTerm < lastLog.Term {
		return
	} else if args.LastLogTerm == lastLog.Term && args.LastLogIndex < lastLog.Index {
		return
	}
	if rf.state.VotedFor == -1 || rf.state.VotedFor == args.CandidateId {
		DPrintf("vote req finsh:%v, me:%d", args, rf.me)
		rf.state.VotedFor = args.CandidateId
		rf.LeaderLastActiveTime = time.Now().UnixMilli()
		reply.VoteGranted = true
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
	PrevLogTerm  int
	Logs         []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term            int
	Success         bool
	TargetNextIndex int
	ConflictTerm    int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("AppendEntries req, me:%d, req: leader:%d, term:%d, len:%d", rf.me, args.LeaderId, args.Term, len(args.Logs))
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if args.Term < rf.state.CurrentTerm {
		reply.Term = rf.state.CurrentTerm
		return
	}
	rf.tranFollower(args.Term)
	rf.LeaderLastActiveTime = time.Now().UnixMilli()
	reply.Term = rf.state.CurrentTerm
	reply.ConflictTerm = -1
	lastLog := &rf.state.Logs[len(rf.state.Logs)-1]
	DPrintf("AppendEntries msg, me:%d, arg:%v, lastLog:%v", rf.me, args, lastLog)
	// reply.TargetNextIndex = args.PrevLogIndex - 1
	// 寻找任期相等的那条日志
	find := func() int {
		for i := len(rf.state.Logs) - 1; i >= 0; i-- {
			if rf.state.Logs[i].Term == args.PrevLogTerm {
				reply.TargetNextIndex = rf.state.Logs[i].Index
				if rf.state.Logs[i].Index == args.PrevLogIndex {
					return i
				}
			} else if rf.state.Logs[i].Term < args.PrevLogTerm {
				// reply.TargetNextIndex = rf.state.Logs[i].Index
				// 旧learder可能有很多未提交和同步的指令 index会很大 使用term查找
				reply.ConflictTerm = rf.state.Logs[i].Term
				break
			}
		}
		return -1
	}
	index := find()
	if index == -1 {
		return
	}
	if len(args.Logs) > 0 {
		rf.state.Logs = append(rf.state.Logs[0:index+1], args.Logs...)
	}
	// rf.ServerData.CommitIndex = args.PrevLogIndex
	reply.Success = true
	if args.LeaderCommit > rf.ServerData.CommitIndex {
		originCommitIndex := rf.ServerData.CommitIndex + 1
		rf.ServerData.CommitIndex = int(math.Min(float64(args.LeaderCommit), float64(rf.state.Logs[len(rf.state.Logs)-1].Index)))
		DPrintf("AppendEntries applymsg, me:%d, curindex:%d, applytoindex:%d, leadercommit:%d",
			rf.me, originCommitIndex, rf.ServerData.CommitIndex, args.LeaderCommit)
		// rf.applymsg()
		rf.applyCond.Signal()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	isLeader = rf.LeaderData != nil
	if !isLeader || rf.killed() {
		return index, term, false
	}
	term = rf.state.CurrentTerm
	index = rf.state.Logs[len(rf.state.Logs)-1].Index + 1
	rf.state.Logs = append(rf.state.Logs, Log{
		Term:    term,
		Index:   index,
		Command: command,
	})
	DPrintf("Start me:%d, index:%d, term:%d, command:%v", rf.me, index, term, command)
	// go rf.recieveCommand()

	return index, term, isLeader
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.state.CurrentTerm
	if args.Term < rf.state.CurrentTerm {
		return
	}
	if rf.tranFollower(args.Term) {
		rf.persist()
	}
	rf.LeaderLastActiveTime = time.Now().UnixMilli()
	if args.LastIncludedIndex <= rf.ServerData.CommitIndex {
		return
	}

	go func() {
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
	}()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
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
	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		timeout := 300 + rf.r.Int63n(200)
		time.Sleep(time.Duration(timeout) * time.Millisecond)
		rf.mu.Lock()
		lastTime := rf.LeaderLastActiveTime
		isLeader := rf.LeaderData != nil
		rf.mu.Unlock()
		now := time.Now().UnixMilli()
		if now < lastTime+200 || isLeader {
			continue
		}
		rf.mu.Lock()
		DPrintf("select leader, %d, term:%d, %d", rf.me, rf.state.CurrentTerm, timeout)
		rf.state.CurrentTerm++
		rf.state.VotedFor = rf.me
		rf.LeaderLastActiveTime = time.Now().UnixMilli()
		req := &RequestVoteArgs{
			Term:         rf.state.CurrentTerm,
			CandidateId:  rf.me,
			LastLogIndex: rf.state.Logs[len(rf.state.Logs)-1].Index,
			LastLogTerm:  rf.state.Logs[len(rf.state.Logs)-1].Term,
		}
		rf.persist()
		rf.mu.Unlock()
		var getTicketNum atomic.Int32
		getTicketNum.Add(1)
		for i := range rf.peers {
			// 不调用自己
			if i == rf.me {
				continue
			}
			go func(server int) {
				reply := &RequestVoteReply{}
				succ := rf.sendRequestVote(server, req, reply)
				// DPrintf("sendRequestVote, me:%d, to:%d", rf.me, server)
				if succ && reply.VoteGranted {
					getTicketNum.Add(1)
					rf.mu.Lock()
					if rf.LeaderData == nil && req.Term == rf.state.CurrentTerm && int(getTicketNum.Load()) > len(rf.peers)/2 {
						DPrintf("to leader, %d, term:%d, ticket:%d",
							rf.me, rf.state.CurrentTerm, getTicketNum.Load())
						serverNum := len(rf.peers)
						rf.LeaderData = &LeaderData{
							NextIndex:  make([]int, serverNum),
							MatchIndex: make([]int, serverNum),
						}
						for i := range rf.LeaderData.NextIndex {
							rf.LeaderData.NextIndex[i] = rf.state.Logs[len(rf.state.Logs)-1].Index + 1
						}
						// 立即广播心跳一次 等待heartBeat最长会等待150ms 可能触发不必要的选举
						rf.broadCastAppendEntries()
					}
					rf.mu.Unlock()
				} else if succ {
					rf.mu.Lock()
					if rf.tranFollower(reply.CurrentTerm) {
						rf.persist()
					}
					rf.mu.Unlock()
				}
			}(i)
		}
	}
}

func (rf *Raft) callAppendEntries(server int) {
	rf.mu.Lock()
	if rf.LeaderData == nil {
		rf.mu.Unlock()
		return
	}
	// Todo nextLogIndex可能越界 为什么？
	nextLogIndex := rf.LeaderData.NextIndex[server] - rf.state.Logs[0].Index
	pre := &rf.state.Logs[len(rf.state.Logs)-1]
	if nextLogIndex < len(rf.state.Logs) {
		pre = &rf.state.Logs[nextLogIndex-1]
	}
	req := &AppendEntriesArgs{
		Term:         rf.state.CurrentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: pre.Index,
		PrevLogTerm:  pre.Term,
		Logs:         []Log{},
		LeaderCommit: rf.ServerData.CommitIndex,
	}
	// 有日志需要同步
	if nextLogIndex < len(rf.state.Logs) {
		req.Logs = rf.state.Logs[nextLogIndex:]
	}
	rf.mu.Unlock()
	reply := &AppendEntriesReply{}
	succ := rf.sendAppendEntries(server, req, reply)
	if succ {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		// 可能rpc返回时 已经不是leader了 需要校验
		if rf.tranFollower(reply.Term) || rf.LeaderData == nil {
			rf.persist()
			return
		}
		if reply.Success {
			if len(req.Logs) > 0 {
				reqIndex := req.Logs[len(req.Logs)-1].Index
				rf.LeaderData.NextIndex[server] = int(math.Max(float64(reqIndex+1), float64(rf.LeaderData.NextIndex[server])))
				rf.LeaderData.MatchIndex[server] = int(math.Max(float64(reqIndex), float64(rf.LeaderData.MatchIndex[server])))
			}
		} else {
			// rf.LeaderData.NextIndex[server] = int(math.Max(1.0, float64(rf.LeaderData.NextIndex[server]-1)))
			if reply.TargetNextIndex > 0 {
				rf.LeaderData.NextIndex[server] = int(math.Max(1.0, float64(reply.TargetNextIndex)))
			} else if reply.ConflictTerm >= 0 {
				for i := range rf.state.Logs {
					if rf.state.Logs[i].Term <= reply.ConflictTerm {
						rf.LeaderData.NextIndex[server] = rf.state.Logs[i].Index + 1
					}
				}
			}

		}
		DPrintf("callAppendEntries leader:%d to:%d, lastIndex: %d, CommitIndex: %d, nextindex:%d matchindex:%d, reply:%v",
			rf.me, server, rf.state.Logs[len(rf.state.Logs)-1].Index, rf.ServerData.CommitIndex,
			rf.LeaderData.NextIndex[server], rf.LeaderData.MatchIndex[server], reply)
	} else {
		DPrintf("callAppendEntries failed leader:%d to:%d", rf.me, server)
	}
}

func (rf *Raft) callInstallSnapshot(server int) {
	rf.mu.Lock()
	req := InstallSnapshotArgs{
		Term:              rf.state.CurrentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.state.Logs[0].Index,
		LastIncludedTerm:  rf.state.Logs[0].Term,
		Offset:            0,
		Data:              rf.Data,
		Done:              true,
	}
	rf.mu.Unlock()
	reply := InstallSnapshotReply{}
	succ := rf.sendInstallSnapshot(server, &req, &reply)
	DPrintf("callInstallSnapshot status:%v, leader:%d, to:%d", succ, rf.me, server)
	rf.mu.Lock()
	if rf.tranFollower(reply.Term) {
		rf.persist()
	}
	rf.mu.Unlock()
}

func (rf *Raft) broadCastAppendEntries() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			rf.mu.Lock()
			if rf.LeaderData == nil {
				rf.mu.Unlock()
				return
			}
			targetIndex := rf.LeaderData.NextIndex[server]
			// Follower需要的日志已经被压缩了
			if targetIndex <= rf.state.Logs[0].Index {
				rf.mu.Unlock()
				rf.callInstallSnapshot(server)
			} else {
				rf.mu.Unlock()
				rf.callAppendEntries(server)
			}
		}(i)
	}
}

func (rf *Raft) leaderTicker() {
	for !rf.killed() {
		rf.mu.Lock()
		isLeader := rf.LeaderData != nil
		if isLeader {
			rf.leaderCheckCommit()
		}
		rf.mu.Unlock()
		if !isLeader {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		// rf.applymsg()
		rf.broadCastAppendEntries()
		time.Sleep(150 * time.Millisecond)
	}
}

func (rf *Raft) leaderCheckCommit() {
	for rf.ServerData.CommitIndex < rf.state.Logs[len(rf.state.Logs)-1].Index {
		commitNum := 1
		for i := range rf.peers {
			if i == rf.me || rf.LeaderData.MatchIndex[i] < rf.ServerData.CommitIndex+1 {
				continue
			}
			commitNum++
			if commitNum > len(rf.peers)/2 {
				rf.ServerData.CommitIndex++
				break
			}
		}
		if commitNum <= len(rf.peers)/2 {
			break
		}
	}
	rf.applyCond.Signal()
}

func (rf *Raft) applymsg() {
	for !rf.killed() {
		rf.mu.Lock()
		// if rf.ServerData.LastApplied >= rf.ServerData.CommitIndex {
		// 	rf.mu.Unlock()
		// 	time.Sleep(10 * time.Millisecond) // 睡眠时间不能过长 影响同步时间 或者改成条件变量形式
		// 	continue
		// }
		for rf.ServerData.LastApplied >= rf.ServerData.CommitIndex {
			rf.applyCond.Wait()
		}
		tmp := make([]Log, 0)
		start := rf.state.Logs[0].Index
		cur := rf.me
		commitIndex := rf.ServerData.CommitIndex
		originCommitIndex := rf.ServerData.LastApplied + 1
		for ; originCommitIndex <= rf.ServerData.CommitIndex; originCommitIndex++ {
			tmp = append(tmp, rf.state.Logs[originCommitIndex-start])
		}
		DPrintf("applymsg me:%d, leader:%v, LastApplied:%d, CommitIndex:%d", rf.me, rf.LeaderData != nil, rf.ServerData.LastApplied, rf.ServerData.CommitIndex)
		rf.mu.Unlock()
		for _, log := range tmp {
			DPrintf("applymsg me:%d, log:%v", cur, log)
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      log.Command,
				CommandIndex: log.Index,
			}
		}
		rf.mu.Lock()
		rf.ServerData.LastApplied = int(math.Max(float64(rf.ServerData.LastApplied), float64(commitIndex)))
		rf.mu.Unlock()
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.applyCh = applyCh
	rf.state.VotedFor = -1
	rf.state.Logs = []Log{{Term: 0}}
	rf.ServerData = &ServerData{}
	rf.r = rand.New(rand.NewSource(time.Now().UnixNano()))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.Data = persister.ReadSnapshot()
	rf.ServerData.LastApplied = rf.state.Logs[0].Index

	// start ticker goroutine to start elections
	go rf.ticker() // 超时选举
	go rf.leaderTicker()
	go rf.applymsg() // 异步 因为applyCh会阻塞造成snapshot时死锁

	return rf
}
