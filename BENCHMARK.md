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
BenchmarkPut1Free/put-24                42995187                26.08 ns/op           43 B/op          0 allocs/op
BenchmarkPutContext1Free/put-24         36235275                32.07 ns/op           41 B/op          0 allocs/op
BenchmarkPut3Free/put-24                16574997                73.87 ns/op          141 B/op          0 allocs/op
BenchmarkPutAll3Free/put-24             37882255                33.78 ns/op          121 B/op          0 allocs/op
BenchmarkPutContext3Free/put-24         12219908                94.60 ns/op          123 B/op          0 allocs/op
BenchmarkPutAllContext3Free/put-24              26843595                40.83 ns/op          136 B/op          0 allocs/op
BenchmarkPutContext1Full/put-24                  1000000              1113 ns/op             320 B/op          5 allocs/op
BenchmarkPutContext3Full/put-24                   370693              3379 ns/op             960 B/op         15 allocs/op
BenchmarkPutAllContext3Full/put-24                337036              3418 ns/op             960 B/op         15 allocs/op
PASS
ok      github.com/mawngo/go-batch      10.862s
```