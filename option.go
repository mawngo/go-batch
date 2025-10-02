package batch

import (
	"golang.org/x/sync/semaphore"
	"math"
	"time"
)

// Unset is a special value for various [Option] functions, usually meaning unrestricted, unlimited, or disable.
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

// WithMaxWait set the max waiting time before the processor will handle the batch anyway.
// If the batch is empty, then it is skipped.
// The max wait start counting from the last processed time, not a fixed period.
// Accept 0 (no wait), -1 [Unset] (wait util maxItems reached), or [time.Duration].
// If set to -1 [Unset] and the maxItems is unlimited,
// then the processor will keep processing whenever possible without waiting for anything.
func WithMaxWait(wait time.Duration) Option {
	return func(p *processorConfig) {
		p.maxWait = wait
		p.isHardMaxWait = false
	}
}

// WithHardMaxWait set the max waiting time before the processor will handle the batch anyway.
// Unlike [WithMaxWait], the batch will be processed even if it is empty,
// which is preferable if the processor must perform some periodic tasks.
// You should ONLY configure [WithMaxWait] OR [WithHardMaxWait], NOT BOTH.
func WithHardMaxWait(wait time.Duration) Option {
	return func(p *processorConfig) {
		p.maxWait = wait
		p.isHardMaxWait = true
	}
}

// WithAggressiveMode enable the aggressive mode.
// In this mode, the processor does not wait for the maxWait or maxItems reached, will continue processing item and only merge into
// batch if needed (for example, reached concurrentLimit, or dispatcher thread is busy).
// The maxItems configured by [WithMaxItem] still control the maximum number of items the processor can hold before block.
// The [WithBlockWhileProcessing] will be ignored in this mode.
func WithAggressiveMode() Option {
	return func(p *processorConfig) {
		p.aggressive = true
	}
}

// WithBlockWhileProcessing enable the processor block when processing item.
// If concurrency enabled, the processor only blocks when reached max concurrency.
// This method has no effect if the processor is in aggressive mode.
func WithBlockWhileProcessing() Option {
	return func(p *processorConfig) {
		p.isBlockWhileProcessing = true
	}
}

// WithDisabledDefaultProcessErrorLog disable default error logging when batch processing error occurs.
func WithDisabledDefaultProcessErrorLog() Option {
	return func(p *processorConfig) {
		p.isDisableErrorLogging = true
	}
}

// WithMaxItem set the max number of items this processor can hold before block.
// Support fixed number and -1 [Unset] (unlimited)
// When set to unlimited, it will never block, and the batch handling behavior depends on [WithMaxWait].
// When set to 0, the processor will be DISABLED and item will be processed directly on caller thread without batching.
func WithMaxItem[I size](maxItem I) Option {
	return func(p *processorConfig) {
		p.maxItem = int64(maxItem)
	}
}

// WithMaxConcurrency set the max number of go routine this processor can create when processing item.
// Support 0 (run on dispatcher goroutine) and fixed number.
// Passing -1 [Unset] (unlimited) to this function has the same effect of passing [math.MaxInt64].
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
	split         SplitBatchFn[B]
	count         func(B, int64) int64
}

// RunOption options for batch processing.
type RunOption[B any] func(*runConfig[B])

// WithBatchCounter provide alternate function to count the number of items in batch.
// The function receives the current batch and the total input items count of the current batch.
func WithBatchCounter[B any](count func(B, int64) int64) RunOption[B] {
	return func(c *runConfig[B]) {
		c.count = count
	}
}

// WithBatchLoaderCountInput unset the current count function.
// Typically used for [Loader] to specify that it should use the number of pending load requests for limit
// instead of the number of pending keys.
func WithBatchLoaderCountInput[K comparable]() RunOption[LoadKeys[K]] {
	return func(c *runConfig[LoadKeys[K]]) {
		c.count = nil
	}
}

// WithBatchSplitter split the batch into multiple smaller batch.
// When concurrency > 0 and [SplitBatchFn] are set,
// the processor will split the batch and process across multiple threads,
// otherwise the batch will be process on a single thread, and block when concurrency is reached.
// This configuration may be beneficial if you have a very large batch that can be split into smaller batch and processed in parallel.
func WithBatchSplitter[B any](split SplitBatchFn[B]) RunOption[B] {
	return func(c *runConfig[B]) {
		c.split = split
	}
}

// WithBatchErrorHandlers provide a [RecoverBatchFn] chain to process on error.
// Each RecoverBatchFn can further return error to enable the next RecoverBatchFn in the chain.
// The RecoverBatchFn must never panic.
func WithBatchErrorHandlers[B any](handlers ...RecoverBatchFn[B]) RunOption[B] {
	return func(c *runConfig[B]) {
		c.errorHandlers = append(c.errorHandlers, handlers...)
	}
}
