# Concurrency problems

Analysis of the library's `.go` files (`batch.go`, `loader.go`, `option.go`, `builtin.go`, `error.go`). Ordered by
severity.

No genuine livelock found: the dispatch loops always make progress toward either processing or an idle wait on
`notEmpty`/`full`/`closed`.
