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
pkg: github.com/mawngo/go-batch/v2
cpu: AMD Ryzen 9 7900 12-Core Processor
BenchmarkPut/PutContext_free1-24                32311917                35.61 ns/op           46 B/op          0 allocs/op
BenchmarkPut/PutContext_free3-24                12044528                97.65 ns/op          124 B/op          0 allocs/op
BenchmarkPut/Put_free1-24                       46476319                22.88 ns/op           40 B/op          0 allocs/op
BenchmarkPut/Put_free3-24                       16808440                69.75 ns/op          139 B/op          0 allocs/op
BenchmarkPut/PutContext_full1-24                 1000000              1087 ns/op             320 B/op          5 allocs/op
BenchmarkPut/PutContext_full3-24                  400545              3223 ns/op             960 B/op         15 allocs/op
BenchmarkPut/Put_full1-24                        1000000              1051 ns/op             320 B/op          5 allocs/op
BenchmarkPut/Put_full3-24                         394099              3132 ns/op             960 B/op         15 allocs/op
BenchmarkPutAll/PutAll_free3-24                 26766356                43.83 ns/op          161 B/op          1 allocs/op
BenchmarkPutAll/PutAllContext_free3-24          20743587                53.65 ns/op          165 B/op          1 allocs/op
BenchmarkPutAll/PutAllContext_full3-24            374821              3201 ns/op             984 B/op         16 allocs/op
BenchmarkPutUnderLoad/PutContext_1000x100x4-24              2973            397881 ns/op           96061 B/op        200 allocs/op
BenchmarkPutUnderLoad/PutContext_1000x100-24                2562            472866 ns/op           89600 B/op        100 allocs/op
BenchmarkPutUnderLoad/Put_1000x100x4-24                     4207            286585 ns/op           96083 B/op        201 allocs/op
BenchmarkPutUnderLoad/Put_1000x100-24                       3609            335089 ns/op           89600 B/op        100 allocs/op
PASS
ok      github.com/mawngo/go-batch/v2   17.770s
```