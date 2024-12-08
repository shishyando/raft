package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"

	rafthttp "shishraft/lib/http"
	raft "shishraft/lib/raft"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

var bgColors = []string{
	"\033[48;5;135m", // Medium orchid
	"\033[48;5;111m", // Steel blue background
	"\033[48;5;94m",  // Brown
	"\033[48;5;106m", // Medium spring green background
	"\033[48;5;218m", // Pink
	"\033[48;5;250m", // Light grey
	"\033[48;5;141m", // Light purple
	"\033[48;5;105m", // Medium purple background
	"\033[48;5;147m", // Light steel blue
	"\033[48;5;196m", // Red
	"\033[48;5;220m", // Gold
	"\033[48;5;46m",  // Green
	"\033[48;5;33m",  // Dark cyan
	"\033[48;5;45m",  // Turquoise
	"\033[48;5;77m",  // Aquamarine
	"\033[48;5;200m", // Hot pink
	"\033[48;5;227m", // Light yellow
}

func main() {

	var (
		raftPort uint
		httpPort uint
		id       int
		nodes    []string
		debug    bool
	)

	flag.UintVar(&raftPort, "raftPort", 1234, "Port for raft replicas")
	flag.UintVar(&httpPort, "httpPort", 1235, "HTTP port for user requests")
	flag.IntVar(&id, "id", 0, "Replica ID")
	flag.BoolVar(&debug, "debug", true, "Use console debug logger")
	flag.Parse()
	nodes = flag.Args()

	zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMs
	zerolog.TimestampFieldName = "ts"
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	if debug {
		msgBgColor := bgColors[id%len(bgColors)]

		zerolog.SetGlobalLevel(zerolog.DebugLevel)
		output := zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: "15:04:05.000"}
		output.FormatMessage = func(i interface{}) string {
			return fmt.Sprintf("%s%s\033[0m", msgBgColor, i)
		}

		log.Logger = zerolog.New(output).With().Int("logger", id).Timestamp().Logger()
	}

	// run raft grpc
	raftServer := raft.RunRaftServer(id, nodes, raftPort)

	// run http server
	httpServer := rafthttp.NewRaftHttpServer(raftServer)
	http.HandleFunc("/op", httpServer.OpHandler)
	http.HandleFunc("/read", httpServer.RedirectReadHandler)

	log.Info().Uint("port", httpPort).Msg("running raft http server")

	if err := http.ListenAndServe(fmt.Sprintf(":%d", httpPort), nil); err != nil {
		log.Fatal().Err(err).Msg("Failed to start http server")
	}

}
