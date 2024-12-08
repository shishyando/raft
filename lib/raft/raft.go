package raft

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sort"
	"sync"
	"time"

	pb "shishraft/lib/proto/pb"

	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
)

func dumpProtoMessageAsText(msg proto.Message) string {
	textBytes, err := prototext.Marshal(msg)
	if err != nil {
		return ""
	}
	return string(textBytes)
}

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

type LogId struct {
	Index int
	Term  int64
}

const (
	RES_CODE_OK = iota
	RES_CODE_ERR
	RES_CODE_NOT_A_LEADER
	RES_CODE_REDIRECT_READ
)

type QueryResponse struct {
	LogId LogId
	Code  int
	Err   error
	Node  string // replica to read from
	Value string // read result
}

type RaftServer struct {
	pb.UnimplementedRaftServer

	results    map[LogId]*chan QueryResponse // channels for http replies
	shouldRead map[LogId]bool                // true for replicas that should read their logs to the result ch

	id    int
	state int // FOLLOWER, CANDIDATE, LEADER
	nodes []string

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

func (s *RaftServer) resetElectionTimer() {
	if s.electionTimer != nil {
		s.electionTimer.Stop()
	}
	timeout := RandomElectionTimeout()
	log.Info().Float64("seconds", timeout.Seconds()).Msg("reset election timer")
	s.electionTimer = time.AfterFunc(timeout, s.elect)
}

func (s *RaftServer) resetHeartbeatTimer() {
	if s.heartbeatTimer != nil {
		s.heartbeatTimer.Stop()
	}
	timeout := RandomHeartBeatTimeout()
	log.Info().Float64("seconds", timeout.Seconds()).Msg("reset heartbeat timer")
	s.heartbeatTimer = time.AfterFunc(timeout, s.heartBeat)
}

func (s *RaftServer) quorum(acks int) bool {
	// s.nodes includes the node itself
	return acks > len(s.nodes)/2
}

func (s *RaftServer) followUnderLock(newTerm int64) {
	log.Info().Msg("follower")
	s.votedFor = -1
	s.currentTerm = newTerm
	s.state = FOLLOWER
	s.resetElectionTimer()
}

func (s *RaftServer) elect() {
	votes := 1

	{ // only update state to CANDIDATE
		s.mu.Lock()
		defer s.mu.Unlock()
		log.Info().Msg("elect")
		if s.state != FOLLOWER {
			return
		}
		s.currentTerm++
		s.state = CANDIDATE
		s.votedFor = s.id
	}

	for i := range s.nodes {
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
			log.Info().Str("req", dumpProtoMessageAsText(req)).Int("target", i).Str("addr", s.nodes[i]).Msg("requesting vote")
			s.mu.Unlock()

			res, err := sendVoteRequest(s.nodes[i], req)
			if err != nil {
				log.Warn().Err(err).Str("Node", s.nodes[i]).Msg("Failed to send vote request")
				return
			}

			{ // account vote or step down, implies state changes
				s.mu.Lock()
				defer s.mu.Unlock()

				if res.Granted { // try to lead
					votes++
					log.Info().Int("votes", votes).Str("response", dumpProtoMessageAsText(res)).Int("follower", i).Msg("got vote response")
					if s.quorum(votes) && s.state == CANDIDATE { // got the quorum, lead
						log.Info().Msg("voted candidate --> leader")
						s.leadUnderLock()
					}
				} else if res.Term > s.currentTerm { // step down
					s.followUnderLock(res.Term)
				}
			}
		}(i)
	}
}

func (s *RaftServer) leadUnderLock() { // reinit leader's state and send the first heartbeat
	log.Info().Int64("Term", s.currentTerm).Msg("lead")

	s.state = LEADER
	s.votedFor = s.id
	for i := range s.nodes {
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
	log.Info().Int("voted for", s.votedFor).Str("req", dumpProtoMessageAsText(req)).Msg("handle RequestVote")

	if req.Term > s.currentTerm {
		log.Info().Int("current term", int(s.currentTerm)).Int("new term", int(req.Term)).Msg("stepping down")
		s.followUnderLock(req.Term)
	}

	if req.Term < s.currentTerm { // make him step down
		response := &pb.VoteResponse{Term: s.currentTerm, Granted: false}
		log.Warn().Str("response", dumpProtoMessageAsText(response)).Msg("Make him step down")
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
		log.Error().Int64("myLastLogTerm", myLastLogTerm).Int64("myLastLogIndex", myLastLogIndex).Str("req", dumpProtoMessageAsText(req))
	}
	if voteNotExhausted && candidateLogIsUpToDate { // worthy
		s.followUnderLock(req.Term)
		s.votedFor = int(req.CandidateId)
		response := &pb.VoteResponse{Term: s.currentTerm, Granted: true}
		log.Info().Str("response", dumpProtoMessageAsText(response)).Msg("Worthy")
		return response, nil
	}

	// unworthy
	response := &pb.VoteResponse{Term: s.currentTerm, Granted: false}
	log.Warn().Str("response", dumpProtoMessageAsText(response)).Bool("voteNotExhausted", voteNotExhausted).Bool("candidateLogIsUpToDate", candidateLogIsUpToDate).Msg("unworthy")
	return response, nil
}

func (s *RaftServer) heartBeat() {
	s.mu.Lock()
	defer s.mu.Unlock()
	log.Info().Msg("heartbeat")

	if s.state != LEADER {
		return
	}

	s.applyOpsUnderLock()

	for i := range s.nodes {
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

				res, err := sendAppendEntriesRequest(s.nodes[i], req)
				if err != nil {
					log.Error().Str("Node", s.nodes[i]).Err(err).Msg("Leader heartbeat failed")
					continue // retry
				}

				s.mu.Lock()
				defer s.mu.Unlock()
				if res.Success {
					s.nextIndex[i] = nextIndex + int64(len(ops))
					s.matchIndex[i] = s.nextIndex[i] - 1
					if s.recalcLeaderCommitIndexUnderLock() {
						s.applyOpsUnderLock()
					}
					return // ok
				} else if res.Term > s.currentTerm {
					s.followUnderLock(res.Term)
					return // stepped down
				}
				// log inconsistency, should retry with greater op suffix
				log.Warn().Str("Node", s.nodes[i]).Msg("Failed to propogate entries because of log inconsistency")
				s.nextIndex[i] = nextIndex - 1
				// retry
			}

		}(i)
	}

	s.resetHeartbeatTimer()
}

func (s *RaftServer) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	log.Info().Str("req", dumpProtoMessageAsText(req)).Msg("AppendEntries")

	if s.currentTerm > req.Term {
		log.Warn().Int("current term", int(s.currentTerm)).Str("Request", dumpProtoMessageAsText(req)).Msg("fake leader that should step down")
		return &pb.AppendEntriesResponse{
			Term:    s.currentTerm,
			Success: false,
		}, nil
	}

	s.resetElectionTimer() // reset timer on heartbeats

	if len(s.log) <= int(req.PrevLogIndex) || // log inconsistency
		(req.PrevLogIndex >= 0 && s.log[req.PrevLogIndex].Term != req.PrevLogTerm) { // log inconsistency

		log.Warn().Int("current term", int(s.currentTerm)).Bool("step down", s.currentTerm > req.Term).Bool("logsize", len(s.log) <= int(req.PrevLogIndex)).Bool("logterm", req.PrevLogIndex >= 0 && s.log[req.PrevLogIndex].Term != req.PrevLogTerm).Str("Request", dumpProtoMessageAsText(req)).Msg("AppendEntries failed")
		return &pb.AppendEntriesResponse{
			Term:    s.currentTerm,
			Success: false,
		}, nil
	} else if s.currentTerm < req.Term {
		s.followUnderLock(req.Term)
		s.votedFor = int(req.LeaderId)
	}

	// append entries to the log (maybe changing uncommitted)
	var next int
	for i, op := range req.LogEntries {
		next = i + 1
		j := int64(i) + req.PrevLogIndex + 1
		if s.log[j].Term != op.Term { // replace
			s.log = append(s.log[:j], req.LogEntries...)
			next = len(req.LogEntries)
			break
		}
	}
	s.log = append(s.log, req.LogEntries[next:]...)

	if req.LeaderCommit > s.commitIndex { // update commit index and apply operations
		s.commitIndex = min(req.LeaderCommit, int64(len(s.log)-1))
		s.applyOpsUnderLock()
	}

	return &pb.AppendEntriesResponse{
		Term:    s.currentTerm,
		Success: true,
	}, nil
}

func (s *RaftServer) AppendOneEntryOnLeader(op *pb.LogEntry) *QueryResponse {
	ch := make(chan QueryResponse)
	{ // append op to log, save a channel for op results
		s.mu.Lock()
		defer s.mu.Unlock()
		log.Info().Str("op", dumpProtoMessageAsText(op)).Msg("append 1 entry on leader")

		if s.state != LEADER {
			return &QueryResponse{LogId: LogId{}, Code: RES_CODE_NOT_A_LEADER, Err: fmt.Errorf("not a leader"), Node: s.nodes[s.votedFor], Value: ""}
		}

		opIndex := len(s.log)
		s.log = append(s.log, op)
		opTerm := s.currentTerm
		s.matchIndex[s.id] = int64(opIndex)
		// if replicated then ok, else fail
		// if read op then send to replica, else direct reply

		// block until op is processed
		logId := LogId{Index: opIndex, Term: opTerm}
		s.results[logId] = &ch
		s.shouldRead[logId] = false
	}

	// now blocking on a channel, unlocking mutex for the leader to replicate ops
	response := <-ch
	return &response
}

func (s *RaftServer) ReadFromReplica(logId LogId) (string, error) {
	var res *chan QueryResponse
	{
		s.mu.Lock()
		defer s.mu.Unlock()
		log.Info().Int("index", logId.Index).Int64("term", logId.Term).Msg("read from replica")
		_, channelAlreadyCreated := s.results[logId]
		if !channelAlreadyCreated {
			ch := make(chan QueryResponse)
			s.results[logId] = &ch
		}
		res = s.results[logId]
	}

	response := <-*res
	return response.Value, response.Err
}

func (s *RaftServer) recalcLeaderCommitIndexUnderLock() bool {
	log.Info().Int("len(matchIndex)", len(s.matchIndex)).Strs("nodes", s.nodes).Msg("update leader's commit index")

	// commitIndex = majority(matchIndex) if term[thatLog] == currentTerm
	values := make([]int64, 0, len(s.matchIndex))
	for _, value := range s.matchIndex {
		values = append(values, value)
	}

	// Descending
	sort.Slice(values, func(i, j int) bool { return values[i] > values[j] })
	n := len(values)
	newCommitIndex := values[n/2+1] // the majority replicated this log entry
	if newCommitIndex > s.commitIndex && len(s.log) > int(newCommitIndex) && s.log[newCommitIndex].Term == s.currentTerm {
		s.commitIndex = newCommitIndex
		return true
	}
	return false
}

func (s *RaftServer) applyOpsUnderLock() {
	log.Info().Msg("apply ops")
	for i, op := range s.log[(s.lastApplied + 1):(s.commitIndex + 1)] {
		logId := LogId{Index: i + int(s.lastApplied) + 1, Term: op.Term}
		if op.OpType == pb.OpType_READ {

			// the leader has already created a channel for results
			// replica did not create any channels

			_, channelAlreadyCreated := s.results[logId]
			if !channelAlreadyCreated {
				ch := make(chan QueryResponse)
				s.results[logId] = &ch
			}
			if s.shouldRead[logId] { // this replica was chosen by the leader to read from
				val, err := Get(&op.Key)
				code := RES_CODE_OK
				if err != nil {
					code = RES_CODE_ERR
				}
				*s.results[logId] <- QueryResponse{LogId: logId, Code: code, Err: err, Node: s.nodes[s.id], Value: val}
				continue
			}

			// this node is the leader so it has to choose where the user should read from
			// choose a replica which commited the log including this read
			node := s.nodes[s.id]
			for i, index := range s.matchIndex {
				if index >= int64(logId.Index) { // this is a valid node to read from
					node = s.nodes[i]
					break
				}
			}
			*s.results[logId] <- QueryResponse{LogId: logId, Code: RES_CODE_REDIRECT_READ, Err: nil, Node: node, Value: ""}
		} else {
			// just return the result
			err := ApplyOp(op)
			code := RES_CODE_OK
			if err != nil {
				code = RES_CODE_ERR
			}
			*s.results[logId] <- QueryResponse{LogId: logId, Code: code, Err: err, Node: s.nodes[s.id], Value: ""}
		}
	}
	s.lastApplied = s.commitIndex
}
