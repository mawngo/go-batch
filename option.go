package batch

import (
	"golang.org/x/sync/semaphore"
	"math"
	"time"
)

// Unset is a special value for various [Option] functions, usually meaning unrestricted, unlimited, or disable.
// You need to read the doc of the corresponding function to know what this value does.
const Unset = -1

// ProcessorConfig configurable options of processor.
type ProcessorConfig struct {
	aggressive             bool
	maxItem                int64
	isBlockWhileProcessing bool
	isDisableErrorLogging  bool
	isHardMaxWait          bool
	concurrentLimit        int64
	concurrentLimiter      *semaphore.Weighted
	maxWait                time.Duration
	maxCloseWait           time.Duration
}

// Option applies an option to [ProcessorConfig].
type Option func(*ProcessorConfig)

// Size is a type alias for int, int32, and int64.
type Size interface {
	~int | ~int32 | ~int64
}

// WithMaxWait set the max waiting time before the processor will handle the batch anyway.
// If the batch is empty, then it is skipped.
// The max wait start counting from the last processed time, not a fixed period.
// Accept 0 (no wait), -1 [Unset] (wait util maxItems reached), or [time.Duration].
// If set to -1 [Unset] and the maxItems is unlimited,
// then the processor will keep processing whenever possible without waiting for anything.
func WithMaxWait(wait time.Duration) Option {
	return func(p *ProcessorConfig) {
		p.maxWait = wait
		p.isHardMaxWait = false
	}
}

// WithHardMaxWait set the max waiting time before the processor will handle the batch anyway.
// Unlike [WithMaxWait], the batch will be processed even if it is empty,
// which is preferable if the processor must perform some periodic tasks.
// You should ONLY configure [WithMaxWait] OR [WithHardMaxWait], NOT BOTH.
func WithHardMaxWait(wait time.Duration) Option {
	return func(p *ProcessorConfig) {
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
	return func(p *ProcessorConfig) {
		p.aggressive = true
	}
}

// WithMaxCloseWait set the max waiting time when closing the processor.
func WithMaxCloseWait(wait time.Duration) Option {
	return func(p *ProcessorConfig) {
		p.maxCloseWait = wait
	}
}

// WithBlockWhileProcessing enable the processor block when processing item.
// If concurrency enabled, the processor only blocks when reached max concurrency.
// This method has no effect if the processor is in aggressive mode.
func WithBlockWhileProcessing() Option {
	return func(p *ProcessorConfig) {
		p.isBlockWhileProcessing = true
	}
}

// WithDisabledDefaultProcessErrorLog disable default error logging when batch processing error occurs.
func WithDisabledDefaultProcessErrorLog() Option {
	return func(p *ProcessorConfig) {
		p.isDisableErrorLogging = true
	}
}

// WithMaxItem set the max number of items this processor can hold before block.
// Support fixed number and -1 [Unset] (unlimited)
// When set to unlimited, it will never block, and the batch handling behavior depends on [WithMaxWait].
// When set to 0, the processor will be DISABLED and item will be processed directly on caller thread without batching.
func WithMaxItem[I Size](maxItem I) Option {
	return func(p *ProcessorConfig) {
		p.maxItem = int64(maxItem)
	}
}

// WithMaxConcurrency set the max number of go routine this processor can create when processing item.
// Support 0 (run on dispatcher goroutine) and fixed number.
// Passing -1 [Unset] (unlimited) to this function has the same effect of passing [math.MaxInt64].
func WithMaxConcurrency[I Size](concurrency I) Option {
	return func(p *ProcessorConfig) {
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
