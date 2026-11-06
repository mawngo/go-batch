package batch

import (
	"golang.org/x/sync/semaphore"
	"math"
	"time"
)

// Unset is a special value for various [Option] functions, usually meaning unrestricted, unlimited, or disabled.
// You need to read the doc of the corresponding function to know what this value does.
const Unset = -1

// processorConfig configurable options of processor.
type processorConfig struct {
	aggressive             bool
	maxItem                int64
	isBlockWhileProcessing bool
	isDisableErrorLogging  bool
	isHardMaxWait          bool
	concurrentLimit        int64
	concurrentLimiter      *semaphore.Weighted
	maxWait                time.Duration
}

// Option general options for batch processor.
type Option func(*processorConfig)

// size is a type alias for int, int32, and int64.
type size interface {
	~int | ~int32 | ~int64
}

// WithMaxWait sets the max waiting time before the processor will handle the batch anyway.
// If the batch is empty, then it is skipped.
// The max wait starts counting from the last processed time, not a fixed period.
// Accept 0 (no wait), -1 [Unset] (wait until maxItem is reached), or [time.Duration].
// If set to -1 [Unset] and maxItem is unlimited,
// then the processor will keep processing whenever possible without waiting for anything.
//
// The default value is 0 (no wait).
func WithMaxWait(wait time.Duration) Option {
	return func(p *processorConfig) {
		p.maxWait = wait
		p.isHardMaxWait = false
	}
}

// WithHardMaxWait sets the max waiting time before the processor will handle the batch anyway.
// Unlike [WithMaxWait], the batch will be processed even if it is empty,
// which is preferable if the processor must perform some periodic tasks.
// You should ONLY configure [WithMaxWait] OR [WithHardMaxWait], NOT BOTH.
//
// See [WithMaxWait].
func WithHardMaxWait(wait time.Duration) Option {
	return func(p *processorConfig) {
		p.maxWait = wait
		p.isHardMaxWait = true
	}
}

// WithAggressiveMode enables the aggressive mode.
// In this mode, the processor does not wait for maxWait or maxItem to be reached; it continues processing items and only merges into a
// batch if needed (for example, when the concurrentLimit is reached or the dispatcher thread is busy).
// The maxItem configured by [WithMaxItem] still controls the maximum number of items the processor can hold before blocking.
// The [WithBlockWhileProcessing] will be ignored in this mode.
//
// This mode can also be enabled by setting maxWait to 0 (no wait) and maxItems to -1 [Unset],
// which is the default configuration for [NewProcessor].
func WithAggressiveMode() Option {
	return func(p *processorConfig) {
		p.aggressive = true
	}
}

// WithBlockWhileProcessing enables the processor to block when processing an item.
// If concurrency is enabled, the processor only blocks when reached max concurrency.
// This method has no effect if the processor is in aggressive mode (see [WithAggressiveMode]).
func WithBlockWhileProcessing() Option {
	return func(p *processorConfig) {
		p.isBlockWhileProcessing = true
	}
}

// WithDisabledDefaultProcessErrorLog disables default error logging when a batch processing error occurs.
func WithDisabledDefaultProcessErrorLog() Option {
	return func(p *processorConfig) {
		p.isDisableErrorLogging = true
	}
}

// WithMaxItem sets the max number of items this processor can hold before blocking.
// Supports a fixed number and -1 [Unset] (unlimited)
// When set to unlimited, it will never block, and the batch handling behavior depends on [WithMaxWait].
// When set to 0, the processor will be DISABLED and items will be processed directly on the caller thread without batching.
//
// The default value is -1 [Unset] (unlimited).
func WithMaxItem[I size](maxItem I) Option {
	return func(p *processorConfig) {
		p.maxItem = int64(maxItem)
	}
}

// WithMaxConcurrency sets the max number of goroutines this processor can create when processing an item.
// Supports 0 (run on the dispatcher goroutine) and a fixed number.
// Passing -1 [Unset] (unlimited) to this function has the same effect as passing [math.MaxInt64].
//
// The default value is 0 (run on dispatcher goroutine).
func WithMaxConcurrency[I size](concurrency I) Option {
	return func(p *processorConfig) {
		p.concurrentLimit = int64(concurrency)
		if concurrency > 0 {
			p.concurrentLimiter = semaphore.NewWeighted(int64(concurrency))
			return
		}
		if concurrency < 0 {
			p.concurrentLimit = math.MaxInt64
			p.concurrentLimiter = semaphore.NewWeighted(math.MaxInt64)
		}
	}
}

// runConfig configurable options for running processor.
// This is a separate type as it contains generic types.
type runConfig[B any] struct {
	errorHandlers []RecoverBatchFn[B]
	splitFn       SplitBatchFn[B]
	countFn       CountBatchFn[B]
}

// RunOption options for batch processing.
type RunOption[B any] func(*runConfig[B])

// WithBatchCounter provides an alternate function to count the number of items in a batch.
func WithBatchCounter[B any](countFn CountBatchFn[B]) RunOption[B] {
	return func(c *runConfig[B]) {
		c.countFn = countFn
	}
}

// WithBatchCountMapKeys provides [WithBatchCounter] with a counter that counts the number of keys in the map.
// The batch must be a map of type map[K]V.
func WithBatchCountMapKeys[K comparable, V any]() RunOption[map[K]V] {
	return func(c *runConfig[map[K]V]) {
		c.countFn = func(m map[K]V) int64 {
			return int64(len(m))
		}
	}
}

// WithLoaderCountKeys provides [WithBatchCounter] with a counter that counts the number of keys of the [Loader].
//
// This option should only be used for [Loader]
// The batch must be LoadKeys[K].
func WithLoaderCountKeys[K any]() RunOption[LoadKeys[K]] {
	return func(c *runConfig[LoadKeys[K]]) {
		c.countFn = func(k LoadKeys[K]) int64 {
			return int64(len(k.Keys))
		}
	}
}

// WithBatchSplitter provides [SplitBatchFn] to split the batch into multiple smaller batches.
// When concurrency > 0 and a [SplitBatchFn] is set,
// the processor will split the batch and process across multiple threads,
// otherwise the batch will be processed on a single thread and block when concurrency is reached.
// This configuration may be beneficial if you have a very large batch that can be split into smaller batches and processed in parallel.
func WithBatchSplitter[B any](split SplitBatchFn[B]) RunOption[B] {
	return func(c *runConfig[B]) {
		c.splitFn = split
	}
}

// WithBatchSplitSliceEqually provides [WithBatchSplitter] with a [SplitBatchFn] that
// splits the batch into multiple equal chunks.
// The batch must be a slice of type T.
func WithBatchSplitSliceEqually[T any, I size](numberOfChunk I) RunOption[[]T] {
	return func(c *runConfig[[]T]) {
		if numberOfChunk <= 0 {
			return
		}

		c.splitFn = func(b []T, _ int64) [][]T {
			n := int64(len(b))
			chunks := int64(numberOfChunk)
			base := n / chunks
			rem := n % chunks
			batches := make([][]T, numberOfChunk)
			start := int64(0)

			for c := range chunks {
				size := base
				if c < rem {
					size++
				}
				end := start + size
				batches[c] = b[start:end]
				start = end
			}
			return batches
		}
	}
}

// WithBatchSplitSliceSizeLimit provides [WithBatchSplitter] with a [SplitBatchFn] that
// splits the batch into multiple chunks of limited size.
// The batch must be a slice of type T.
func WithBatchSplitSliceSizeLimit[T any, I size](maxSizeOfChunk I) RunOption[[]T] {
	return func(c *runConfig[[]T]) {
		if maxSizeOfChunk <= 0 {
			return
		}

		c.splitFn = func(b []T, _ int64) [][]T {
			n := int64(len(b))
			chunks := (n + int64(maxSizeOfChunk) - 1) / int64(maxSizeOfChunk)
			batches := make([][]T, 0, chunks)
			for start := int64(0); start < n; start += int64(maxSizeOfChunk) {
				end := min(start+int64(maxSizeOfChunk), n)
				batches = append(batches, b[start:end])
			}
			return batches
		}
	}
}

// WithBatchErrorHandlers provides a [RecoverBatchFn] chain to process on error.
// Each RecoverBatchFn can further return an error to enable the next RecoverBatchFn in the chain.
// The RecoverBatchFn must never panic.
func WithBatchErrorHandlers[B any](handlers ...RecoverBatchFn[B]) RunOption[B] {
	return func(c *runConfig[B]) {
		c.errorHandlers = handlers
	}
}
