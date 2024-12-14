go build -o raft_server ./bin/main.go

killall raft_server

trap "kill 0" SIGINT

./raft_server --id 0 --basePort 5050 localhost localhost localhost &
./raft_server --id 1 --basePort 5050 localhost localhost localhost &
./raft_server --id 2 --basePort 5050 localhost localhost localhost &

wait
