# Concurrency problems

Analysis of the library's `.go` files (`batch.go`, `loader.go`, `option.go`, `builtin.go`, `error.go`). Ordered by severity.

## Deadlocks & goroutine leaks

### 1. `Stop` deadlocks a producer blocked on `p.full`
When the batch reaches `maxItem`, `Merge`/`MergeAll` block on `p.full <- struct{}{}` (`batch.go:371`, `batch.go:455`) **while holding the `blocked` lock**. `Stop` (`batch.go:506`) only signals `closed` and returns — it never drains. The dispatcher exits, nobody is left to receive `full`, so the producer (and the `blocked` lock) is stuck forever.

### 2. `Close` with an already-canceled context leaves `blocked` held
`Close` -> `Drain` can return on `ctx.Done()` (`batch.go:553`) before consuming a pending `full`, so the producer stuck on `full` also holds `blocked`, freezing all subsequent `Put`/`Merge`/`ItemCount`. Acknowledged in `batch_test.go:19-23` (`goleak` ignores it).

## Data races (undefined behavior under `-race`)

### 3. `Future` fields read/written without the loader lock
`Future.Get`/`IsDone` (`loader.go:361-375`, `378-389`) read `r.ch`, `r.result`, `r.err` unlocked. Writes occur under `l.lock` in `processLoad` (`loader.go:192-197`) and `Stop` (`loader.go:332-334`). `close(ch)` gives happens-before only for the `<-r.ch` path; the `if r.ch == nil` check and the `ctx.Done()` early return (`loader.go:371`) are unsynchronized. The `future.ch = nil` write also races with `Get`/`IsDone` reads.

### 4. `l.loading` map is written without the loader lock (concurrent map access)
`Load` and `LoadAll` insert `l.loading[key] = future` (`loader.go:240`, `loader.go:308`) **without holding `l.lock`**. The comment at `loader.go:232-233`/`300-301` assumes this is safe because the merge callback runs under the processor's `blocked` lock — but `processLoad` (`loader.go:182-198`) and `Stop` (`loader.go:331-336`) mutate the map under `l.lock`, a *different* lock. In the default non-blocking mode (`doProcessAndRelease(false)` releases `blocked` before calling `processLoad`), a concurrent map write/delete is possible → runtime `fatal error: concurrent map writes` and a data race.

---

No genuine livelock found: the dispatch loops always make progress toward either processing or an idle wait on `notEmpty`/`full`/`closed`.
