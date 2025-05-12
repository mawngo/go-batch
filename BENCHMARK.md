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
BenchmarkPut1Free/put-24                44828139                24.23 ns/op           41 B/op          0 allocs/op
BenchmarkPutContext1Free/put-24         34623241                31.20 ns/op           43 B/op          0 allocs/op
BenchmarkPut3Free/put-24                16271760                70.97 ns/op          144 B/op          0 allocs/op
BenchmarkPutAll3Free/put-24             34782003                32.34 ns/op          131 B/op          0 allocs/op
BenchmarkPutContext3Free/put-24         11623908                95.73 ns/op          129 B/op          0 allocs/op
BenchmarkPutAllContext3Free/put-24              27978288                39.24 ns/op          131 B/op          0 allocs/op
PASS
ok      github.com/mawngo/go-batch      6.920s
```