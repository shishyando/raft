package raft

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sort"
	"sync"
	"time"

	"shishraft/lib/helpers"
	"shishraft/lib/proto/pb"
	"shishraft/lib/storage"

	"github.com/rs/zerolog/log"
)

const (
	FOLLOWER = iota
	CANDIDATE
	LEADER
)

func RandomElectionTimeout() time.Duration {
	return time.Millisecond * time.Duration(3000+rand.IntN(3000))
}

func RandomHeartBeatTimeout() time.Duration {
	return time.Millisecond * time.Duration(1000+rand.IntN(1000))
}

type ApplyOpResult struct {
	LogIndex int
	Redirect bool   // redirect query to node?
	Node     string // replica to get operation result from
	Err      error
	Val      string
}

type RaftServer struct {
	pb.UnimplementedRaftServer

	results map[int]*chan ApplyOpResult // channels for operation results

	id        int
	state     int // FOLLOWER, CANDIDATE, LEADER
	raftNodes []string
	httpNodes []string

	currentTerm int64
	votedFor    int
	log         []*pb.LogEntry
	commitIndex int64
	lastApplied int64

	// Reinitialized at election
	nextIndex  map[int]int64
	matchIndex map[int]int64

	// System
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer

	mu sync.Mutex
}

func (s *RaftServer) safeResetElectionTimer() {
	if s.electionTimer != nil {
		s.electionTimer.Stop()
	}
	timeout := RandomElectionTimeout()
	log.Debug().Float64("seconds", timeout.Seconds()).Msg("reset election timer")
	s.electionTimer = time.AfterFunc(timeout, s.elect)
}

func (s *RaftServer) resetHeartbeatTimer() {
	if s.heartbeatTimer != nil {
		s.heartbeatTimer.Stop()
	}
	timeout := RandomHeartBeatTimeout()
	log.Debug().Float64("seconds", timeout.Seconds()).Msg("reset heartbeat timer")
	s.heartbeatTimer = time.AfterFunc(timeout, s.heartBeat)
}

func (s *RaftServer) quorum(acks int) bool {
	// s.nodes includes the node itself
	return acks > len(s.raftNodes)/2
}

// should be called under lock
func (s *RaftServer) safeCreateResChan(id int) *chan ApplyOpResult {
	chptr, exists := s.results[id]
	if exists {
		log.Info().Int("opIndex", id).Str("chptr", fmt.Sprintf("%p", chptr)).Msg("safeCreateResChan: chan already created")
		return chptr
	}
	ch := make(chan ApplyOpResult, 1)
	s.results[id] = &ch
	log.Info().Int("opIndex", id).Str("chptr", fmt.Sprintf("%p", &ch)).Msg("safeCreateResChan: new chan created")
	return &ch
}

// should be called under lock
func (s *RaftServer) safeFollow(newTerm int64) {
	log.Debug().Msg("follower")
	s.votedFor = -1
	s.currentTerm = newTerm
	s.state = FOLLOWER
	s.safeResetElectionTimer()
}

func (s *RaftServer) elect() {
	votes := 1

	{ // only update state to CANDIDATE
		s.mu.Lock()
		defer s.mu.Unlock()
		log.Debug().Msg("elect")
		if s.state != FOLLOWER {
			return
		}
		s.currentTerm++
		s.state = CANDIDATE
		s.votedFor = s.id
	}

	for i := range s.raftNodes {
		if i == s.id {
			continue
		}
		go func(i int) {
			lastLogIndex := int64(len(s.log))
			var lastLogTerm int64
			if len(s.log) > 0 {
				lastLogTerm = s.log[len(s.log)-1].Term
			}
			req := &pb.VoteRequest{
				Term:         s.currentTerm,
				CandidateId:  int32(s.id),
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			s.mu.Lock()
			log.Debug().Str("req", helpers.DumpProtoMessageAsText(req)).Int("target", i).Str("addr", s.raftNodes[i]).Msg("requesting vote")
			s.mu.Unlock()

			res, err := sendVoteRequest(s.raftNodes[i], req)
			if err != nil {
				log.Warn().Err(err).Str("Node", s.raftNodes[i]).Msg("Failed to send vote request, will retry on next heartbeat")
				return
			}

			{ // account vote or step down, implies state changes
				s.mu.Lock()
				defer s.mu.Unlock()

				if res.Granted { // try to lead
					votes++
					log.Debug().Int("votes", votes).Str("response", helpers.DumpProtoMessageAsText(res)).Int("follower", i).Msg("got vote response")
					if s.quorum(votes) && s.state == CANDIDATE { // got the quorum, lead
						log.Debug().Msg("voted candidate --> leader")
						s.safeLead()
					}
				} else if res.Term > s.currentTerm { // step down
					s.safeFollow(res.Term)
				}
			}
		}(i)
	}
}

// should be called under lock
func (s *RaftServer) safeLead() { // reinit leader's state and send the first heartbeat
	log.Info().Int64("Term", s.currentTerm).Msg("lead")

	s.state = LEADER
	s.votedFor = s.id
	for i := range s.raftNodes {
		s.nextIndex[i] = int64(len(s.log)) // nothing to replicate
		if i == s.id {
			s.matchIndex[i] = int64(len(s.log)) // already replicated the whole log on leader
		} else {
			s.matchIndex[i] = -1
		}
	}

	go s.heartBeat()
}

func (s *RaftServer) RequestVote(ctx context.Context, req *pb.VoteRequest) (*pb.VoteResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	log.Debug().Int("voted for", s.votedFor).Str("req", helpers.DumpProtoMessageAsText(req)).Msg("handle RequestVote")

	if req.Term > s.currentTerm {
		log.Debug().Int("current term", int(s.currentTerm)).Int("new term", int(req.Term)).Msg("stepping down")
		s.safeFollow(req.Term)
		// should not grant the vote yet
	} else if req.Term < s.currentTerm { // make him step down
		response := &pb.VoteResponse{Term: s.currentTerm, Granted: false}
		log.Warn().Str("response", helpers.DumpProtoMessageAsText(response)).Msg("Make him step down")
		return response, nil
	}

	// check if he's worthy
	myLastLogIndex := int64(len(s.log)) - 1
	var myLastLogTerm int64
	if myLastLogIndex > 0 {
		myLastLogTerm = s.log[myLastLogIndex].Term
	}

	voteNotExhausted := s.votedFor == -1 || s.votedFor == int(req.CandidateId)
	candidateLogIsUpToDate := myLastLogTerm < req.LastLogTerm || (req.LastLogTerm == myLastLogTerm && myLastLogIndex <= req.LastLogIndex)
	if !candidateLogIsUpToDate {
		log.Error().Int64("myLastLogTerm", myLastLogTerm).Int64("myLastLogIndex", myLastLogIndex).Str("req", helpers.DumpProtoMessageAsText(req))
	}
	if voteNotExhausted && candidateLogIsUpToDate { // worthy
		s.safeFollow(req.Term)
		s.votedFor = int(req.CandidateId)
		response := &pb.VoteResponse{Term: s.currentTerm, Granted: true}
		log.Debug().Str("response", helpers.DumpProtoMessageAsText(response)).Msg("Worthy")
		return response, nil
	}

	// unworthy
	response := &pb.VoteResponse{Term: s.currentTerm, Granted: false}
	log.Debug().Str("response", helpers.DumpProtoMessageAsText(response)).Bool("voteNotExhausted", voteNotExhausted).Bool("candidateLogIsUpToDate", candidateLogIsUpToDate).Msg("unworthy")
	return response, nil
}

func (s *RaftServer) heartBeat() {
	s.mu.Lock()
	defer s.mu.Unlock()
	log.Debug().Msg("heartbeat")

	if s.state != LEADER {
		return
	}
	s.votedFor = s.id

	s.safeApplyOps()

	for i := range s.raftNodes {
		if i == s.id {
			continue
		}
		go func(i int) {
			for {
				// fill request
				s.mu.Lock()
				nextIndex := s.nextIndex[i]

				ops := s.log[nextIndex:]
				prevLogIndex := nextIndex - 1
				var prevLogTerm int64
				if prevLogIndex >= 0 {
					prevLogTerm = s.log[prevLogIndex].Term
				}
				req := &pb.AppendEntriesRequest{
					Term:         s.currentTerm,
					LeaderId:     int32(s.id),
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					LogEntries:   ops,
					LeaderCommit: s.commitIndex,
				}
				s.mu.Unlock()

				res, err := sendAppendEntriesRequest(s.raftNodes[i], req)
				if err != nil {
					log.Error().Str("Node", s.raftNodes[i]).Err(err).Msg("Leader heartbeat failed")
					continue // retry
				}
				log.Debug().Str("response", helpers.DumpProtoMessageAsText(res)).Msg("got AppendEntries response")

				s.mu.Lock()
				defer s.mu.Unlock()
				if res.Success {
					s.nextIndex[i] = nextIndex + int64(len(ops))
					s.matchIndex[i] = s.nextIndex[i] - 1
					if s.safeRecalcLeaderCommitIndex() {
						s.safeApplyOps()
					}
					return // ok
				} else if res.Term > s.currentTerm {
					s.safeFollow(res.Term)
					return // stepped down
				}
				// log inconsistency, should retry with greater op suffix
				log.Debug().Str("Node", s.raftNodes[i]).Msg("Failed to propogate entries because of log inconsistency")
				s.nextIndex[i] = nextIndex - 1
				// retry
			}

		}(i)
	}

	log.Info().Msg("hb")
	s.resetHeartbeatTimer()
}

func (s *RaftServer) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	log.Debug().Str("req", helpers.DumpProtoMessageAsText(req)).Msg("AppendEntries")

	if s.currentTerm > req.Term {
		log.Warn().Int("current term", int(s.currentTerm)).Str("Request", helpers.DumpProtoMessageAsText(req)).Msg("fake leader that should step down")
		return &pb.AppendEntriesResponse{
			Term:    s.currentTerm,
			Success: false,
		}, nil
	}
	if s.currentTerm < req.Term {
		s.safeFollow(req.Term)
	} else {
		s.safeResetElectionTimer() // reset timer on heartbeats
	}

	if len(s.log) <= int(req.PrevLogIndex) || // log inconsistency
		(req.PrevLogIndex >= 0 && s.log[req.PrevLogIndex].Term != req.PrevLogTerm) { // log inconsistency

		log.Debug().Bool("logsize inconsistency", len(s.log) <= int(req.PrevLogIndex)).Bool("logterm inconsistency", req.PrevLogIndex >= 0 && s.log[req.PrevLogIndex].Term != req.PrevLogTerm).Str("Request", helpers.DumpProtoMessageAsText(req)).Msg("AppendEntries failed")
		return &pb.AppendEntriesResponse{
			Term:    s.currentTerm,
			Success: false,
		}, nil
	}
	s.votedFor = int(req.LeaderId)

	// append entries to the log (maybe changing uncommitted)

	for k, op := range req.LogEntries {
		j := int64(k) + req.PrevLogIndex + 1
		if int(j) >= len(s.log) || s.log[j].Term != op.Term {
			s.log = append(s.log[:min(len(s.log), int(j))], req.LogEntries[k:]...)
			break
		}
	}

	if req.LeaderCommit > s.commitIndex { // update commit index and apply operations
		s.commitIndex = min(req.LeaderCommit, int64(len(s.log)-1))
		s.safeApplyOps()
		log.Debug().Int64("commit index", s.commitIndex).Int64("applied", s.lastApplied).Msg("ops applied!")
	}

	log.Debug().Msg("AppendEntries success")
	return &pb.AppendEntriesResponse{
		Term:    s.currentTerm,
		Success: true,
	}, nil
}

func (s *RaftServer) safeRecalcLeaderCommitIndex() bool {

	// commitIndex = majority(matchIndex) if term[thatLog] == currentTerm
	values := make([]int64, 0, len(s.matchIndex))
	for _, value := range s.matchIndex {
		values = append(values, value)
	}
	log.Debug().Ints64("matchIndexes", values).Msg("update leader's commit index")
	// Descending
	sort.Slice(values, func(i, j int) bool { return values[i] > values[j] })
	n := len(values)
	newCommitIndex := values[n/2+1] // the majority replicated this log entry
	log.Debug().Int64("new commit index", newCommitIndex).Int64("current commit index", s.commitIndex).Int("len(log)", len(s.log)).Int64("current term", s.currentTerm).Msg("try to update commit index")
	if len(s.log) > int(newCommitIndex) && newCommitIndex >= 0 {
		log.Debug().Int64("new commit index log term", s.log[newCommitIndex].Term).Msg("comparing terms...")
	}
	if newCommitIndex > s.commitIndex && len(s.log) > int(newCommitIndex) && s.log[newCommitIndex].Term == s.currentTerm {
		s.commitIndex = newCommitIndex
		log.Debug().Msg("Updated commit index")
		return true
	}
	log.Debug().Msg("commit index up to date")
	return false
}

// should be called under lock
func (s *RaftServer) safeApplyOps() {
	log.Debug().Int64("last applied", s.lastApplied).Int64("commit index", s.commitIndex).Msg("apply ops")
	for i, op := range s.log[(s.lastApplied + 1):(s.commitIndex + 1)] {
		opIndex := i + int(s.lastApplied) + 1
		chPtr := s.safeCreateResChan(opIndex)
		log.Info().Int("opIndex", opIndex).Str("op", helpers.DumpProtoMessageAsText(op)).Str("chPtr", fmt.Sprintf("%p", chPtr)).Msg("Applying op")
		val, err := storage.ApplyOp(op)
		*chPtr <- ApplyOpResult{Val: val, Err: err, LogIndex: opIndex}
	}
	s.lastApplied = s.commitIndex
}

func (s *RaftServer) AppendOneEntryOnLeader(op *pb.LogEntry) ApplyOpResult {
	// append op to log, save a channel for op results
	s.mu.Lock()
	if s.state != LEADER {
		defer s.mu.Unlock()
		if s.votedFor == -1 {
			log.Warn().Str("op", helpers.DumpProtoMessageAsText(op)).Msg("No leader known")
			return ApplyOpResult{Err: fmt.Errorf("no leader known")}
		}
		log.Warn().Str("leader http", s.httpNodes[s.votedFor]).Str("op", helpers.DumpProtoMessageAsText(op)).Msg("Tried to append 1 entry on a follower")
		return ApplyOpResult{LogIndex: -1, Redirect: true, Node: s.httpNodes[s.votedFor]}
	}
	op.Term = s.currentTerm
	opIndex := len(s.log)
	s.log = append(s.log, op)
	s.matchIndex[s.id] = int64(opIndex)
	resChPtr := s.safeCreateResChan(opIndex)

	log.Info().Str("op", helpers.DumpProtoMessageAsText(op)).Msg("append 1 entry on leader")
	s.mu.Unlock()

	// blocking until the operation is replicated
	res := <-*resChPtr
	logApplyOpResult(res, "leader got op result")
	if op.OpType == pb.OpType_READ { // find a replica to answer instead of master
		res.Redirect = true
		res.Val = "" // save network banwidth, redirect reading to some up-to-date replica
		s.mu.Lock()
		defer s.mu.Unlock()
		for nodeId, lastIndex := range s.matchIndex {
			if lastIndex >= int64(opIndex) && s.id != nodeId {
				res.Node = s.httpNodes[nodeId]
				break
			}
		}
	}

	logApplyOpResult(res, "fixed result")
	return res
}

func (s *RaftServer) DirectReadById(id int) ApplyOpResult {
	s.mu.Lock()
	log.Info().Int("id", id).Msg("DirectReadById")
	resChPtr := s.safeCreateResChan(id)
	log.Info().Str("chPtr", fmt.Sprintf("%p", resChPtr)).Msg("DirectReadById waiting on ptr")
	s.mu.Unlock()
	res := <-*resChPtr
	logApplyOpResult(res, "DirectReadById")
	return res
}

func logApplyOpResult(res ApplyOpResult, msg string) {
	errStr := ""
	if res.Err != nil {
		errStr = res.Err.Error()
	}
	log.Info().Str("addr", res.Node).Str("err", errStr).Int("index", res.LogIndex).Bool("redirect", res.Redirect).Str("val", res.Val).Msg(msg)
}
