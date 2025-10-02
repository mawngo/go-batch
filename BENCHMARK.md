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
pkg: github.com/mawngo/go-batch/v3
cpu: AMD Ryzen 9 7900 12-Core Processor             
BenchmarkPut/Put_free1-24       31850599                35.04 ns/op           47 B/op          0 allocs/op
BenchmarkPut/Put_free3-24       11150270               100.4 ns/op           134 B/op          0 allocs/op
BenchmarkPut/Put_full1-24        1000000              1139 ns/op             320 B/op          5 allocs/op
BenchmarkPut/Put_full3-24         354078              3419 ns/op             960 B/op         15 allocs/op
BenchmarkPutAll/PutAll_free3-24                 27954303                44.83 ns/op          131 B/op          0 allocs/op
BenchmarkPutAll/PutAll_full3-24                   349328              3369 ns/op             960 B/op         15 allocs/op
BenchmarkPutUnderLoad/Put_1000x100x4-24             2804            429045 ns/op           96042 B/op        200 allocs/op
BenchmarkPutUnderLoad/Put_1000x100-24               2521            477324 ns/op           89601 B/op        100 allocs/op
PASS
ok      github.com/mawngo/go-batch/v3   9.693s
```