go build -o raft_server ./bin/main.go

killall raft_server

trap "kill 0" SIGINT

./raft_server -id 0 -raftPort 5050 -httpPort 5051 localhost:5050 localhost:5052 localhost:5054 localhost:5056 localhost:5058 &
./raft_server -id 1 -raftPort 5052 -httpPort 5053 localhost:5050 localhost:5052 localhost:5054 localhost:5056 localhost:5058 &
./raft_server -id 2 -raftPort 5054 -httpPort 5055 localhost:5050 localhost:5052 localhost:5054 localhost:5056 localhost:5058 &
./raft_server -id 3 -raftPort 5056 -httpPort 5057 localhost:5050 localhost:5052 localhost:5054 localhost:5056 localhost:5058 &
./raft_server -id 4 -raftPort 5058 -httpPort 5059 localhost:5050 localhost:5052 localhost:5054 localhost:5056 localhost:5058 &

wait
