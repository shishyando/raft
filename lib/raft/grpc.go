package raft

import (
	"context"
	"fmt"
	"net"
	"time"

	"shishraft/lib/proto/pb"

	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const grpcTimeout time.Duration = time.Millisecond * time.Duration(5000)

func RunRaftServer(id int, nodes []string, port uint) *RaftServer {
	server := &RaftServer{
		results:    make(map[LogId]*chan QueryResponse),
		shouldRead: make(map[LogId]bool),

		id:    id,
		state: FOLLOWER,
		nodes: nodes,

		currentTerm: 0,
		votedFor:    -1,
		log:         make([]*pb.LogEntry, 0),
		commitIndex: -1,
		lastApplied: -1,

		nextIndex:  make(map[int]int64, len(nodes)),
		matchIndex: make(map[int]int64, len(nodes)),
	}
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))

	if err != nil {
		log.Fatal().Err(err).Msg("RunRaftServer failed")
	}

	grpcServer := grpc.NewServer()
	pb.RegisterRaftServer(grpcServer, server)
	log.Info().Uint("Port", port).Strs("Nodes", nodes).Msg("Running Raft grpc server")

	server.resetElectionTimer()
	go grpcServer.Serve(lis)
	return server
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
