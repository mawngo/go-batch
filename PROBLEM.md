# Concurrency problems

Analysis of the library's `.go` files (`batch.go`, `loader.go`, `option.go`, `builtin.go`, `error.go`). Ordered by
severity.

## Deadlocks & goroutine leaks

### 1. `StopContext` deadlocks a producer blocked on `p.full`

When the batch reaches `maxItem`, `MergeContext`/`MergeAllContext` block on `p.full <- struct{}{}` **while holding the
`blocked`
lock**. `Stop` only signals `closed` and returns — it never drains. The dispatcher exits, nobody is left to receive
`full`, so the producer (and the `blocked` lock) is stuck forever.

### 2. `Close` with an already-canceled context leaves `blocked` held

`Close` -> `Drain` can return on `ctx.Done()` before consuming a pending `full`, so the producer stuck
on `full` also holds `blocked`, freezing all subsequent `Put`/`Merge`/`ItemCount`.

### 3. Item can still be added to the batch after `Close`

---
No genuine livelock found: the dispatch loops always make progress toward either processing or an idle wait on
`notEmpty`/`full`/`closed`.
