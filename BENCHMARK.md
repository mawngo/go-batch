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
BenchmarkPut/PutContext_free1-24                32585672                34.36 ns/op           46 B/op          0 allocs/op
BenchmarkPut/PutContext_free3-24                12240550               100.8 ns/op           122 B/op          0 allocs/op
BenchmarkPut/Put_free1-24                       47440204                23.95 ns/op           49 B/op          0 allocs/op
BenchmarkPut/Put_free3-24                       14629953                68.66 ns/op          128 B/op          0 allocs/op
BenchmarkPut/PutContext_full1-24                 1000000              1054 ns/op             320 B/op          5 allocs/op
BenchmarkPut/PutContext_full3-24                  368942              3249 ns/op             960 B/op         15 allocs/op
BenchmarkPut/Put_full1-24                        1000000              1042 ns/op             320 B/op          5 allocs/op
BenchmarkPut/Put_full3-24                         366759              3131 ns/op             960 B/op         15 allocs/op
BenchmarkPutAll/PutAll_free3-24                 35902130                33.77 ns/op          127 B/op          0 allocs/op
BenchmarkPutAll/PutAllContext_free3-24          28360546                40.48 ns/op          129 B/op          0 allocs/op
BenchmarkPutAll/PutAllContext_full3-24            364135              3276 ns/op             960 B/op         15 allocs/op
BenchmarkPutUnderLoad/PutContext_1000x100x4-24              2929            406602 ns/op           96105 B/op        201 allocs/op
BenchmarkPutUnderLoad/PutContext_1000x100-24                2514            485473 ns/op           89601 B/op        100 allocs/op
BenchmarkPutUnderLoad/Put_1000x100x4-24                     4164            290593 ns/op           96065 B/op        201 allocs/op
BenchmarkPutUnderLoad/Put_1000x100-24                       3356            367640 ns/op           89601 B/op        100 allocs/op
PASS
ok      github.com/mawngo/go-batch      17.591s
```