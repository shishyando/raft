package raft

import (
	"context"
	"fmt"
	"net"
	"time"

	"shishraft/lib/proto/pb"
	"shishraft/lib/storage"

	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const grpcTimeout time.Duration = time.Millisecond * time.Duration(5000)

func NewRaftServer(id int, raftAddrs []string, httpAddrs []string) *RaftServer {
	server := &RaftServer{
		results: make(map[int]*chan ApplyOpResult),
		storage: storage.NewStorage(),

		id:        id,
		state:     FOLLOWER,
		raftNodes: raftAddrs,
		httpNodes: httpAddrs,

		currentTerm: 0,
		votedFor:    -1,
		log:         make([]*pb.LogEntry, 0),
		commitIndex: -1,
		lastApplied: -1,

		nextIndex:  make(map[int]int64, len(raftAddrs)),
		matchIndex: make(map[int]int64, len(raftAddrs)),
	}
	log.Info().Strs("Nodes", raftAddrs).Msg("Created a raft grpc server")
	return server
}

func (s *RaftServer) RunRaftServer(raftPort uint) {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", raftPort))
	if err != nil {
		log.Fatal().Uint("port", raftPort).Err(err).Msg("failed to listen for grpc server")
	}
	grpcServer := grpc.NewServer()
	pb.RegisterRaftServer(grpcServer, s)
	log.Info().Uint("Port", raftPort).Msg("Running Raft grpc server")

	s.safeResetElectionTimer()
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatal().Uint("port", raftPort).Err(err).Msg("failed to run grpc server")
	}
}

func sendVoteRequest(node string, request *pb.VoteRequest) (*pb.VoteResponse, error) {
	conn, err := grpc.NewClient(node, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	client := pb.NewRaftClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), grpcTimeout)
	defer cancel()

	return client.RequestVote(ctx, request)
}

func sendAppendEntriesRequest(node string, request *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	conn, err := grpc.NewClient(node, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	client := pb.NewRaftClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), grpcTimeout)
	defer cancel()

	return client.AppendEntries(ctx, request)
}
