# Concurrency problems

Analysis of the library's `.go` files (`batch.go`, `loader.go`, `option.go`, `builtin.go`, `error.go`). Ordered by severity.

## Deadlocks & goroutine leaks

### 1. `Stop` deadlocks a producer blocked on `p.full`
When the batch reaches `maxItem`, `Merge`/`MergeAll` block on `p.full <- struct{}{}` (`batch.go:371`, `batch.go:455`) **while holding the `blocked` lock**. `Stop` (`batch.go:506`) only signals `closed` and returns — it never drains. The dispatcher exits, nobody is left to receive `full`, so the producer (and the `blocked` lock) is stuck forever.

### 2. `Close` with an already-canceled context leaves `blocked` held
`Close` -> `Drain` can return on `ctx.Done()` (`batch.go:553`) before consuming a pending `full`, so the producer stuck on `full` also holds `blocked`, freezing all subsequent `Put`/`Merge`/`ItemCount`. Acknowledged in `batch_test.go:19-23` (`goleak` ignores it).

---

No genuine livelock found: the dispatch loops always make progress toward either processing or an idle wait on `notEmpty`/`full`/`closed`.
