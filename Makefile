alice:
	go run raft.go -name alice

bob:
	go run raft.go -name bob

charlie:
	go run raft.go -name charlie

dave:
	go run raft.go -name dave -n 5

eric:
	go run raft.go -name eric -n 5

clean:
	rm -rf /tmp/raftdb
	rm log-*
