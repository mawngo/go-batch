package batch

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

var _ Runner[any, any] = (*RunningProcessor[any, any])(nil)

// ProcessorSetup batch processor that is in setup phase (not running)
// You cannot put item into this processor, use Run to create a RunningProcessor that can accept item.
// See ProcessorConfig for available options.
type ProcessorSetup[T any, B any] struct {
	ProcessorConfig
	merge MergeToBatchFn[B, T]
	init  InitBatchFn[B]
	split SplitBatchFn[B]
}

// Runner provides common methods of a RunningProcessor.
type Runner[T any, B any] interface {
	// Put add item to the processor.
	// This method can block until the processor is available for processing new batch,
	// and may block indefinitely.
	Put(item T)
	// PutContext add item to the processor.
	// If the context is canceled and the item is not added, then this method will return false.
	// The context passed in only control the put step, after item added to the processor,
	// the processing will not be canceled by this context.
	PutContext(ctx context.Context, item T) bool
	// ApproxItemCount return number of current item in processor, approximately.
	ApproxItemCount() int64
	// ItemCount return number of current item in processor.
	ItemCount() int64
	// ItemCountContext return number of current item in processor.
	// If the context is canceled, then this method will return approximate item count and false.
	ItemCountContext(ctx context.Context) (int64, bool)
	// Close stop the processor.
	// The implementation of this method may vary, but it must never wait indefinitely.
	Close() error
	// CloseContext stop the processor.
	// This method may process the left-over batch on caller thread.
	// ctx can be used to provide deadline for this method.
	CloseContext(ctx context.Context) error
	// StopContext stop the processor.
	// This method does not process leftover batch.
	StopContext(ctx context.Context) error
	// DrainContext force process batch util the batch is empty.
	// This method may process the batch on caller thread.
	// ctx can be used to provide deadline for this method.
	DrainContext(ctx context.Context) error
	// FlushContext force process the current batch.
	// This method may process the batch on caller thread.
	// ctx can be used to provide deadline for this method.
	FlushContext(ctx context.Context) error
	// Flush force process the current batch.
	// This method may process the batch on caller thread.
	Flush()
	// MustClose stop the processor and panic if there is any error.
	// This method should only be used in tests.
	MustClose()
}

// SliceRunner shorthand for runner that merge item into slices.
type SliceRunner[T any] interface {
	Runner[T, []T]
}

// MapRunner shorthand for runner that merge item into maps.
type MapRunner[K comparable, T any] interface {
	Runner[T, map[K]T]
}

// RunningProcessor processor that is running and can process item.
type RunningProcessor[T any, B any] struct {
	ProcessorSetup[T, B]
	process       ProcessBatchFn[B]
	errorHandlers []RecoverBatchFn[B]
	batch         B
	counter       int64

	empty   chan struct{}
	full    chan struct{}
	blocked chan struct{}
	closed  chan struct{}
}

// InitBatchFn function to create empty batch.
type InitBatchFn[B any] func(int64) B

// ProcessBatchFn function to process a batch.
type ProcessBatchFn[B any] func(B, int64) error
type ProcessBatchIgnoreErrorFn[B any] func(B, int64)

// SplitBatchFn function to split a batch into multiple smaller batches.
// The SplitBatchFn must never panic.
type SplitBatchFn[B any] func(B, int64) []B

// RecoverBatchFn function to handle an error batch.
// Each RecoverBatchFn can further return error to enable the next RecoverBatchFn in the chain.
// The RecoverBatchFn must never panic.
type RecoverBatchFn[B any] func(B, int64, error) error

// MergeToBatchFn function to add an item to batch.
type MergeToBatchFn[B any, T any] func(B, T) B

// Error is an error wrapper that supports passing remaining items to the RecoverBatchFn.
type Error[B any] struct {
	// Cause the error cause. If not specified, then nil will be passed to the next error handler.
	Cause error
	// RemainingBatch the batch to pass to the next handler. The RemainingCount must be specified.
	RemainingBatch B
	// RemainingCount number of items to pass to the next handler.
	// If RemainingCount = 0 and Cause != nil then pass the original batch and count to the next handler.
	RemainingCount int64
}

func (e *Error[B]) Error() string {
	return e.Cause.Error()
}

func (e *Error[B]) String() string {
	return e.Cause.Error()
}

// NewErrorWithRemaining create a *Error with remaining items.
func NewErrorWithRemaining[B any](err error, remainBatch B, count int64) error {
	return &Error[B]{
		Cause:          err,
		RemainingBatch: remainBatch,
		RemainingCount: count,
	}
}

// LoggingErrorHandler default error handler, always included in RecoverBatchFn chain unless disable.
func LoggingErrorHandler[B any](_ B, count int64, err error) error {
	slog.Error("error processing batch", slog.Any("count", count), slog.Any("err", err))
	return err
}

// NewProcessor create a ProcessorSetup using specified functions.
// See Configure and Option for available configuration.
// The result ProcessorSetup is in setup state. Call ProcessorSetup.Run with a handler to create a RunningProcessor that can accept item.
// It is recommended to set at least maxWait or maxItem.
// By default, the processor operates similarly to aggressive mode, use Configure to change its behavior.
func NewProcessor[T any, B any](init InitBatchFn[B], merge MergeToBatchFn[B, T]) ProcessorSetup[T, B] {
	c := ProcessorConfig{
		maxWait: 0,
		maxItem: Disabled,
	}
	return ProcessorSetup[T, B]{
		ProcessorConfig: c,
		init:            init,
		merge:           merge,
	}
}

// NewSliceProcessor create processor that backed by a slice.
func NewSliceProcessor[T any]() ProcessorSetup[T, []T] {
	return NewProcessor(InitSlice[T], AddToSlice[T])
}

// NewMapProcessor create processor that backed by a map.
func NewMapProcessor[T any, K comparable, V any](keyExtractor Extractor[T, K], valueExtractor Extractor[T, V], combiner Combine[V]) ProcessorSetup[T, map[K]V] {
	return NewProcessor(InitMap[K, V], MergeToMapUsing(keyExtractor, valueExtractor, combiner))
}

// NewIdentityMapProcessor create processor that backed by a map, using item as value without extracting.
func NewIdentityMapProcessor[T any, K comparable](keyExtractor Extractor[T, K], combiner Combine[T]) ProcessorSetup[T, map[K]T] {
	return NewProcessor(InitMap[K, T], MergeSelfToMapUsing(keyExtractor, combiner))
}

// NewReplaceIdentityMapProcessor create processor that backed by a map, using item as value without extracting.
// ProcessorSetup created by this construct handles duplicated key by keeping only the last value.
func NewReplaceIdentityMapProcessor[T any, K comparable](keyExtractor Extractor[T, K]) ProcessorSetup[T, map[K]T] {
	return NewProcessor(InitMap[K, T], AddSelfToMapUsing(keyExtractor))
}

// NewReplaceMapProcessor create processor that backed by a map.
// ProcessorSetup created by this construct handles duplicated key by keeping only the last value.
func NewReplaceMapProcessor[T any, K comparable, V any](keyExtractor Extractor[T, K], valueExtractor Extractor[T, V]) ProcessorSetup[T, map[K]V] {
	return NewProcessor(InitMap[K, V], AddToMapUsing(keyExtractor, valueExtractor))
}

// Configure apply Option to this processor.
// Each Configure call creates a new processor.
func (p ProcessorSetup[T, B]) Configure(options ...Option) ProcessorSetup[T, B] {
	for i := range options {
		options[i](&p.ProcessorConfig)
	}
	return p
}

// ItemCount return number of current item in processor.
func (p *RunningProcessor[T, B]) ItemCount() int64 {
	cnt, _ := p.ItemCountContext(context.Background())
	return cnt
}

// ItemCountContext return number of current item in processor.
func (p *RunningProcessor[T, B]) ItemCountContext(ctx context.Context) (int64, bool) {
	select {
	case p.blocked <- struct{}{}:
	case <-ctx.Done():
		return p.counter, false
	}
	defer func() { <-p.blocked }()
	return p.counter, true
}

// ApproxItemCount return number of current item in processor.
// This method does not block, so the counter may not accurate.
func (p *RunningProcessor[T, B]) ApproxItemCount() int64 {
	return p.counter
}

func (p ProcessorSetup[T, B]) RunIgnoreError(process ProcessBatchIgnoreErrorFn[B]) *RunningProcessor[T, B] {
	return p.Run(func(b B, i int64) error {
		process(b, i)
		return nil
	})
}

// WithSplitter split the batch into multiple smaller batch.
// When concurrency > 0 and SplitBatchFn are set, the processor will split the batch and process across multiple thread,
// otherwise the batch will be process on a single thread, and block when concurrency is reached.
// This configuration may be beneficial if you have a very large batch that can be split into smaller batch and processed in parallel.
func (p ProcessorSetup[T, B]) WithSplitter(split SplitBatchFn[B]) ProcessorSetup[T, B] {
	p.split = split
	return p
}

func (p ProcessorSetup[T, B]) isAggressiveMode() bool {
	return p.aggressive || (p.maxWait == 0 && p.maxItem < 0)
}

// Run create a RunningProcessor that can accept item.
// Accept a ProcessBatchFn and a RecoverBatchFn chain to process on error.
func (p ProcessorSetup[T, B]) Run(process ProcessBatchFn[B], errorHandlers ...RecoverBatchFn[B]) *RunningProcessor[T, B] {
	// if errorHandlers is empty, then add a default logging handler.
	if !p.isDisableErrorLogging && len(errorHandlers) == 0 {
		errorHandlers = []RecoverBatchFn[B]{LoggingErrorHandler[B]}
	}

	processor := &RunningProcessor[T, B]{
		ProcessorSetup: p,
		process:        process,
		errorHandlers:  errorHandlers,
		full:           make(chan struct{}),
		blocked:        make(chan struct{}, 1),
		closed:         make(chan struct{}),
	}

	if p.isAggressiveMode() {
		processor.maxWait = 0
		processor.isBlockWhileProcessing = false
		processor.empty = make(chan struct{}, 1)
	}

	processor.batch = p.init(p.maxItem)
	if processor.IsDisabled() {
		return processor
	}

	if p.maxWait < 0 && p.maxItem >= 0 {
		processor.waitUtilFullDispatch()
		return processor
	}

	if p.maxWait == 0 {
		if processor.empty != nil {
			processor.continuousDispatch()
		} else {
			processor.waitUtilFullContinuousDispatch()
		}
		return processor
	}

	processor.timedDispatch()
	return processor
}

// continuousDispatch create a dispatcher routine that, when batch is empty, wait util it not empty, else process the remaining batch util it became empty.
func (p *RunningProcessor[T, B]) continuousDispatch() {
	if p.empty == nil {
		// Should never happen.
		panic("Empty channel is nil. This is a bug in the lib!")
	}
	go func() {
		for {
			select {
			case p.blocked <- struct{}{}:
				// if empty then try to wait util non-empty.
				if p.counter == 0 {
					<-p.blocked
					select {
					case <-p.empty:
						select {
						// re-acquire the lock.
						case p.blocked <- struct{}{}:
							p.doProcessAndRelease(p.isBlockWhileProcessing)
						case <-p.full:
							p.doProcessAndRelease(p.isBlockWhileProcessing)
						case <-p.closed:
							return
						}
					case <-p.full:
						p.doProcessAndRelease(p.isBlockWhileProcessing)
					case <-p.closed:
						return
					}
					break
				}
				p.doProcessAndRelease(p.isBlockWhileProcessing)
			case <-p.full:
				p.doProcessAndRelease(p.isBlockWhileProcessing)
			case <-p.closed:
				return
			}
		}
	}()
}

// waitUtilFullContinuousDispatch create a dispatcher routine that, when batch is empty, wait util it full, else process the remaining batch util it became empty.
// maxItem must be specified.
func (p *RunningProcessor[T, B]) waitUtilFullContinuousDispatch() {
	go func() {
		for {
			select {
			case p.blocked <- struct{}{}:
				// if not empty then process anyway.
				if p.counter > 0 {
					p.doProcessAndRelease(p.isBlockWhileProcessing)
					break
				}
				<-p.blocked
				// if empty then wait util full to process.
				select {
				case <-p.full:
					p.doProcessAndRelease(p.isBlockWhileProcessing)
				case <-p.closed:
					return
				}
			case <-p.full:
				p.doProcessAndRelease(p.isBlockWhileProcessing)
			case <-p.closed:
				return
			}
		}
	}()
}

// timedDispatch create a dispatcher routine that wait util the batch is full or AT LEAST maxWait elapsed.
// when maxWait is passed and the batch is empty, it will reset the timer to avoid processing only one item.
func (p *RunningProcessor[T, B]) timedDispatch() {
	go func() {
		for {
			timer := time.NewTimer(p.maxWait)
			select {
			case <-timer.C:
				select {
				case p.blocked <- struct{}{}:
					// if empty, then reset the timer.
					// this avoids the first item getting processed immediately after a long wait.
					if p.counter == 0 && !p.isHardMaxWait {
						<-p.blocked
						break
					}
					p.doProcessAndRelease(p.isBlockWhileProcessing)
				case <-p.full:
					p.doProcessAndRelease(p.isBlockWhileProcessing)
				case <-p.closed:
					return
				}
			case <-p.full:
				if !timer.Stop() {
					<-timer.C
				}
				p.doProcessAndRelease(p.isBlockWhileProcessing)
			case <-p.closed:
				if !timer.Stop() {
					<-timer.C
				}
				return
			}
		}
	}()
}

// waitUtilFullDispatch create a dispatcher routine that wait util the batch is full.
// maxItem must be specified.
func (p *RunningProcessor[T, B]) waitUtilFullDispatch() {
	go func() {
		for {
			select {
			case <-p.full:
				p.doProcessAndRelease(p.isBlockWhileProcessing)
			case <-p.closed:
				return
			}
		}
	}()
}

// IsDisabled whether the processor is disabled.
// Disabled processor won't do batching, instead the process will be executed on caller.
// All other settings are ignored when the processor is disabled.
func (p *RunningProcessor[T, B]) IsDisabled() bool {
	return p.maxItem == 0
}

// Put add item to the processor.
// Put add item to the processor.
// This method can block until the processor is available for processing new batch.
func (p *RunningProcessor[T, B]) Put(item T) {
	p.PutContext(nil, item)
}

// PutContext add item to the processor.
func (p *RunningProcessor[T, B]) PutContext(ctx context.Context, item T) bool {
	if ctx != nil && ctx.Err() != nil {
		return false
	}

	if p.IsDisabled() {
		batch := p.merge(p.init(1), item)
		p.doProcess(batch, 1)
		return true
	}

	// Select is slow, and most of the old codes are using Put without Context,
	// so we allow nil context to preserve performance.
	if ctx != nil {
		select {
		case <-ctx.Done():
			return false
		case p.blocked <- struct{}{}:
		}
	} else {
		p.blocked <- struct{}{}
	}

	// Always release in case of panic.
	defer func() {
		if r := recover(); r != nil {
			select {
			case <-p.blocked:
			default:
			}
			panic(r)
		}
	}()

	p.batch = p.merge(p.batch, item)
	p.counter++
	if p.empty != nil {
		select {
		case p.empty <- struct{}{}:
			// notify that the batch is now not empty.
		default:
			// processing, no need to modify.
		}
	}
	if p.maxItem > -1 && p.counter >= p.maxItem {
		// Block util processed.
		p.full <- struct{}{}
		return true
	}
	<-p.blocked
	return true
}

// Close stop the processor.
// This method will process the leftover branch on caller thread.
// Return error if maxCloseWait passed. See getCloseMaxWait for detail.
func (p *RunningProcessor[T, B]) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), p.getCloseMaxWait())
	defer cancel()
	return p.CloseContext(ctx)
}

// MustClose stop the processor without deadline.
func (p *RunningProcessor[T, B]) MustClose() {
	err := p.CloseContext(context.Background())
	if err != nil {
		panic(err)
	}
}

// CloseContext stop the processor.
// This method will process the leftover branch on caller thread.
// ctx can be used to provide deadline for this method.
func (p *RunningProcessor[T, B]) CloseContext(ctx context.Context) error {
	if p.IsDisabled() {
		return nil
	}
	p.closed <- struct{}{}
	slog.Debug("waiting for leftover batch to finish")
	if err := p.DrainContext(ctx); err != nil {
		return err
	}
	if p.concurrentLimiter != nil {
		slog.Debug("waiting for concurrent workers to finish")
		err := p.concurrentLimiter.Acquire(ctx, p.concurrentLimit)
		if err != nil {
			slog.Error("error waiting for concurrent workers to finish", slog.Any("err", err))
			return err
		}
		p.concurrentLimiter.Release(p.concurrentLimit)
	}
	return nil
}

// StopContext stop the processor.
// This method does not process leftover batch.
func (p *RunningProcessor[T, B]) StopContext(ctx context.Context) error {
	if p.IsDisabled() {
		return nil
	}
	p.closed <- struct{}{}
	if p.concurrentLimiter != nil {
		slog.Debug("waiting for concurrent workers to finish")
		err := p.concurrentLimiter.Acquire(ctx, p.concurrentLimit)
		if err != nil {
			slog.Error("error waiting for concurrent workers to finish", slog.Any("err", err))
			return err
		}
		p.concurrentLimiter.Release(p.concurrentLimit)
	}
	return nil
}

// DrainContext force process batch util the batch is empty.
// This method always processes the batch on caller thread.
// ctx can be used to provide deadline for this method.
func (p *RunningProcessor[T, B]) DrainContext(ctx context.Context) error {
	if p.IsDisabled() {
		return nil
	}
	waiting := true
	for waiting {
		select {
		case <-p.full:
			p.doProcessAndRelease(true)
		case p.blocked <- struct{}{}:
			if p.counter == 0 {
				<-p.blocked
				waiting = false
				break
			}
			// Process the remaining items.
			batch := p.batch
			counter := p.counter
			p.batch = p.init(p.maxItem)
			p.counter = 0
			<-p.blocked
			p.doProcessConcurrency(batch, counter)
		case <-ctx.Done():
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return context.DeadlineExceeded
		}
	}
	return nil
}

// FlushContext force process the current batch.
// This method may process the batch on caller thread, depend on concurrent and block settings.
// ctx can be used to provide deadline for this method.
func (p *RunningProcessor[T, B]) FlushContext(ctx context.Context) error {
	if p.IsDisabled() {
		return nil
	}
	select {
	case <-p.full:
		p.doProcessAndRelease(p.isBlockWhileProcessing)
	case p.blocked <- struct{}{}:
		if p.counter == 0 {
			<-p.blocked
			return nil
		}
		p.doProcessAndRelease(p.isBlockWhileProcessing)
	case <-ctx.Done():
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return context.DeadlineExceeded
	}
	return nil
}

// Flush force process the current batch.
// This method may process the batch on caller thread, depend on concurrent and block settings.
func (p *RunningProcessor[T, B]) Flush() {
	err := p.FlushContext(context.Background())
	if err != nil {
		// should never have happened.
		slog.Error("error flushing batch", slog.Any("err", err))
	}
}

func (p *RunningProcessor[T, B]) getCloseMaxWait() time.Duration {
	if p.maxCloseWait > 0 {
		return p.maxCloseWait
	}
	// if maxWait is set then wait double the time.
	if p.maxWait > 0 {
		return p.maxWait * 2
	}
	return 15 * time.Second
}

func (p *RunningProcessor[T, B]) doProcessAndRelease(block bool) {
	batch := p.batch
	counter := p.counter
	p.batch = p.init(p.maxItem)
	p.counter = 0
	if block {
		defer func() {
			if r := recover(); r != nil {
				<-p.blocked
				panic(r)
			}
		}()
		p.doProcessConcurrency(batch, counter)
		<-p.blocked
		return
	}
	<-p.blocked
	p.doProcessConcurrency(batch, counter)
}

func (p *RunningProcessor[T, B]) doProcessConcurrency(batch B, counter int64) {
	if p.concurrentLimiter == nil {
		p.doProcess(batch, counter)
		return
	}

	if p.split == nil {
		p.doAcquireThreadAndProcess(batch, counter)
		return
	}

	batches := p.split(batch, counter)
	for i := range batches {
		p.doAcquireThreadAndProcess(batches[i], counter)
	}
}

func (p *RunningProcessor[T, B]) doAcquireThreadAndProcess(batch B, counter int64) {
	err := p.concurrentLimiter.Acquire(context.TODO(), 1)
	if err != nil {
		slog.Error("error acquiring worker to process batch", slog.Any("err", err))
		return
	}
	go func() {
		defer p.concurrentLimiter.Release(1)
		p.doProcess(batch, counter)
	}()
}

func (p *RunningProcessor[T, B]) doProcess(batch B, counter int64) {
	defer func() {
		if r := recover(); r != nil {
			err := fmt.Errorf("panic: %v", r)
			p.doHandleError(batch, counter, err)
		}
	}()
	err := p.process(batch, counter)
	if err != nil {
		p.doHandleError(batch, counter, err)
	}
}

func (p *RunningProcessor[T, B]) doHandleError(batch B, counter int64, err error) {
	e := &Error[B]{}
	for i := 0; err != nil && i < len(p.errorHandlers); i++ {
		handler := p.errorHandlers[i]
		if errors.As(err, &e) {
			err = e.Cause
			if e.RemainingCount > 0 {
				counter = e.RemainingCount
				batch = e.RemainingBatch
			}
		}
		err = handler(batch, counter, err)
	}
}
