package main

import (
	"flag"
	"fmt"
	"net/http"

	"shishraft/lib/helpers"
	rafthttp "shishraft/lib/http"
	"shishraft/lib/raft"

	"github.com/gorilla/mux"
	"github.com/rs/zerolog/log"
)

func main() {

	var (
		basePort  uint // raftPort = basePort, httpPort = basePort + 1
		id        int
		addrs     []string
		local     bool
		debug     bool
		colorById bool
	)

	flag.UintVar(&basePort, "basePort", 5050, "Raft port = base port + 2*id, HTTP port = base port + 2*id + 1")
	flag.IntVar(&id, "id", 0, "0-indexed node id")
	flag.BoolVar(&debug, "debug", false, "Set log level to debug")
	flag.BoolVar(&local, "local", true, "Use console debug logger")
	flag.BoolVar(&colorById, "colorById", true, "Color log messages by node id")
	flag.Parse()
	addrs = flag.Args()

	helpers.SetupLogger(id, local, debug, colorById)

	// run raft grpc
	raftAddrs, httpAddrs := helpers.GetRaftAndHttpAddrs(addrs, basePort)
	raftServer := raft.RunRaftServer(id, raftAddrs, httpAddrs, helpers.GetRaftPort(id, basePort))

	// run http server
	httpServer := rafthttp.NewRaftHttpServer(raftServer)
	httpPort := helpers.GetHttpPort(id, basePort)
	log.Info().Uint("port", httpPort).Msg("running raft http server")

	r := mux.NewRouter()

	r.HandleFunc("/create", httpServer.CreateHandler).Methods(http.MethodPost)
	r.HandleFunc("/read/{key}", httpServer.ReadHandler).Methods(http.MethodGet)
	r.HandleFunc("/read/{key}/{logId:[0-9]+}", httpServer.ReadByIdHandler).Methods(http.MethodGet)
	r.HandleFunc("/update", httpServer.UpdateHandler).Methods(http.MethodPut)
	r.HandleFunc("/delete/{key}", httpServer.DeleteHandler).Methods(http.MethodDelete)
	r.HandleFunc("/cas", httpServer.CasHandler).Methods(http.MethodPut)

	if err := http.ListenAndServe(fmt.Sprintf(":%d", httpPort), r); err != nil {
		log.Fatal().Err(err).Msg("Failed to start http server")
	}

}
