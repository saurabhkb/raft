alice:
	go run *.go -name alice

bob:
	go run *.go -name bob

charlie:
	go run *.go -name charlie

clean:
	rm -rf /tmp/raftdb
