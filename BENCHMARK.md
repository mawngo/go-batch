# Benchmark

Overhead when using this library

## Run

```shell
go test -bench=BenchmarkPut -benchmem -benchtime=1s -run=^$
```

## Result

```
❯ go test -bench=BenchmarkPut -benchmem -benchtime=1s -run=^$
goos: windows
goarch: amd64
pkg: github.com/mawngo/go-batch
cpu: AMD Ryzen 9 7900 12-Core Processor
BenchmarkPutFree/put-24         44923292                24.30 ns/op           41 B/op          0 allocs/op
BenchmarkPutContextFree/put-24          36917396                30.97 ns/op           40 B/op          0 allocs/op
PASS
ok      github.com/mawngo/go-batch      2.446s
```