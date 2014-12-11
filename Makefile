alice:
	go run *.go -name alice

bob:
	go run *.go -name bob

charlie:
	go run *.go -name charlie

dave:
	go run *.go -name dave -n 5

eric:
	go run *.go -name eric -n 5

clean:
	rm -rf /tmp/raftdb
