test:
	@rm -rf /tmp/xbase
	GOCACHE=off go test -v ./xbase   -test.run=TestPut
	GOCACHE=off go test -v ./xbase   -test.run=Test2Put
	GOCACHE=off go test -v ./xbase   -test.run=TestGet
	GOCACHE=off go test -v ./xbase   -test.bench=BenchmarkPutPerf
	GOCACHE=off go test -v ./xbase   -test.run=TestDel
	GOCACHE=off go test -v ./xbase   -test.run=Test2Del
	GOCACHE=off go test -v ./xbase   -test.run=Test3DelFromOldBranch

.phony: test
