package raft_test

import (
	"fmt"
	"net"
	"os"
	"slices"
	"testing"
	"time"

	"shishraft/lib/helpers"
	rafthttp "shishraft/lib/http"
	"shishraft/lib/proto/pb"
	"shishraft/lib/raft"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
)

const (
	BasePort    = 5250
	ClusterSize = 5
)

func SetupTestLogger() {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMs
	zerolog.TimestampFieldName = "ts"
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	output := zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: "15:04:05.000"}
	log.Logger = zerolog.New(output).With().Timestamp().Logger()
}

type StandManager struct {
	raftServer *raft.RaftServer
	grpcServer *grpc.Server
	httpServer *rafthttp.HttpServer
	raftPort   uint
	httpPort   uint
	active     bool
}

func NewStandManager(id int, addrs []string) *StandManager {
	raftAddrs, httpAddrs := helpers.GetRaftAndHttpAddrs(addrs, BasePort)
	raftServer := raft.NewRaftServer(id, raftAddrs, httpAddrs)
	httpServer := rafthttp.NewRaftHttpServer(raftServer)
	return &StandManager{
		raftServer: raftServer,
		httpServer: httpServer,

		raftPort: helpers.GetRaftPort(id, BasePort),
		httpPort: helpers.GetHttpPort(id, BasePort),
		active:   false,
	}
}

func (s *StandManager) ActivateStand(afterHalt bool) {
	grpcServer := grpc.NewServer()
	pb.RegisterRaftServer(grpcServer, s.raftServer)
	s.grpcServer = grpcServer

	if afterHalt {
		s.raftServer.ResetAfterHalt()
	} else {
		s.raftServer.Reset()
	}

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.raftPort))
	if err != nil {
		log.Fatal().Err(err).Uint("raft port", s.raftPort).Msg("failed to listen")
	}

	go func() {
		log.Info().Uint("raft port", s.raftPort).Msg("start raft server")
		if err := s.grpcServer.Serve(lis); err != nil {
			log.Fatal().Err(err).Uint("raft port", s.raftPort).Msg("failed to serve")
		}
	}()
	s.active = true
}

func (s *StandManager) PauseStand() {
	log.Warn().Int("id", s.raftServer.GetId()).Msg("pausing stand")
	s.grpcServer.GracefulStop()
	s.raftServer.Halt()
	s.active = false
}

func GetClusterLeader(stands []*StandManager) (int, error) {
	leaderIds := []int{}
	for _, stand := range stands {
		if stand.active {
			standLeader := stand.raftServer.GetLeaderId()
			leaderIds = append(leaderIds, standLeader)
		}
	}
	for _, leader := range leaderIds {
		if leader != leaderIds[0] {
			return -1, fmt.Errorf("leaders inconsistency: %v", leaderIds)
		}
	}
	return leaderIds[0], nil
}

func NewTestCluster(totalNodes int) []*StandManager {
	addrs := slices.Repeat([]string{"localhost"}, totalNodes)
	stands := make([]*StandManager, totalNodes)
	for i := 0; i < totalNodes; i++ {
		stands[i] = NewStandManager(i, addrs)
		stands[i].ActivateStand(false)
	}

	return stands
}

// just some leader should be elected and reelected after failures
func TestLeaderElection(t *testing.T) {
	SetupTestLogger()

	stands := NewTestCluster(ClusterSize)

	time.Sleep(15 * time.Second)

	// leader elected
	leader1, err := GetClusterLeader(stands)
	assert.NoError(t, err)

	stands[leader1].PauseStand()
	time.Sleep(15 * time.Second)

	leader2, err := GetClusterLeader(stands)
	assert.NoError(t, err)
	stands[leader2].PauseStand()
	time.Sleep(15 * time.Second)

	leader3, err := GetClusterLeader(stands)
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, leader3, 0)
	assert.Less(t, leader3, ClusterSize)

	stands[leader1].ActivateStand(true)
	stands[leader2].ActivateStand(true)
	time.Sleep(15 * time.Second)

	leader4, err := GetClusterLeader(stands)
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, leader4, 0)
	assert.Less(t, leader4, ClusterSize)
}

// replicate simple `create` entry
func TestLogReplication(t *testing.T) {
	SetupTestLogger()

	stands := NewTestCluster(ClusterSize)
	time.Sleep(15 * time.Second)

	vals := []string{"val1", "val2"}
	ops := []*pb.LogEntry{
		{
			OpType: pb.OpType_CREATE,
			Key:    "key1",
			Value:  &vals[0],
		},
		{
			OpType: pb.OpType_CREATE,
			Key:    "key2",
			Value:  &vals[1],
		},
	}

	for _, op := range ops {
		leader, err := GetClusterLeader(stands)
		assert.NoError(t, err)
		res := stands[leader].raftServer.AppendOneEntryOnLeader(op)
		assert.NoError(t, res.Err)
		time.Sleep(15 * time.Second)
		for _, stand := range stands {
			if stand.active {
				standLog := stand.raftServer.GetLog()
				lastOp := standLog[len(standLog)-1]
				assert.Equal(t, op.OpType, lastOp.OpType)
				assert.Equal(t, op.Key, lastOp.Key)
				assert.Equal(t, *op.Value, *lastOp.Value)
			}
		}
		stands[leader].PauseStand()
		time.Sleep(15 * time.Second)
	}

}
