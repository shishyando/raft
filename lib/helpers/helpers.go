package helpers

import (
	"fmt"
	"os"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
)

func DumpProtoMessageAsText(msg proto.Message) string {
	textBytes, err := prototext.Marshal(msg)
	if err != nil {
		return ""
	}
	return string(textBytes)
}


func GetRaftPort(id int, basePort uint) uint {
	return basePort + 2*uint(id)
}
func GetHttpPort(id int, basePort uint) uint {
	return basePort + 2*uint(id) + 1
}

func GetRaftAndHttpAddrs(addrs []string, basePort uint) ([]string, []string) {
	raftAddrs := make([]string, len(addrs))
	httpAddrs := make([]string, len(addrs))
	for i, addr := range addrs {
		raftAddrs[i] = fmt.Sprintf("%s:%d", addr, 2*uint(i)+basePort)
		httpAddrs[i] = fmt.Sprintf("%s:%d", addr, 1+2*uint(i)+basePort)
	}
	return raftAddrs, httpAddrs
}

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

func SetupLogger(id int, local bool, debug bool, colorById bool) {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMs
	zerolog.TimestampFieldName = "ts"
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	if debug {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	}
	if local {
		msgBgColor := bgColors[id%len(bgColors)]

		output := zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: "15:04:05.000"}
		if colorById {
			output.FormatMessage = func(i interface{}) string {
				return fmt.Sprintf("%s%s\033[0m", msgBgColor, i)
			}
		}

		log.Logger = zerolog.New(output).With().Int("logger", id).Timestamp().Logger()
	}
}
