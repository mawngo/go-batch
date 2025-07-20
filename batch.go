package batch

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

var _ Processor[any, any] = (*runningProcessor[any, any])(nil)

// ProcessorSetup batch processor that is in setup phase (not running).
// You cannot put item into this processor, use [ProcessorSetup.Run] to create a [Runner] that can accept item.
// See [Option] for available options.
type ProcessorSetup[T any, B any] struct {
	processorConfig
	merge MergeToBatchFn[B, T]
	init  InitBatchFn[B]
}

// Deprecated: to be removal.
type Runner[T any, B any] = Processor[T, B]

// Deprecated: to be removal.
type SliceRunner[T any] = Processor[T, []T]

// Deprecated: to be removal.
type MapRunner[K comparable, T any] = Processor[T, map[K]T]

// Processor provides common methods of a processor.
type Processor[T any, B any] interface {
	// Put add item to the processor.
	// This method can block until the processor is available for processing new batch,
	// and may block indefinitely.
	// It is recommended to use [Processor.PutContext] instead.
	Put(item T)
	// PutAll add all specified item to the processor.
	// This method can block until the processor is available for processing new batch,
	// and may block indefinitely.
	// It is recommended to use [Processor.PutAllContext] instead.
	PutAll(items []T)
	// PutContext add item to the processor.
	// If the context is canceled and the item is not added, then this method will return false.
	// The context passed in only control the put step, after item added to the processor,
	// the processing will not be canceled by this context.
	PutContext(ctx context.Context, item T) bool
	// PutAllContext add all items to the processor.
	// If the context is canceled, then this method will return the number of items added to the processor.
	PutAllContext(ctx context.Context, items []T) int

	// Merge add item to the processor using merge function.
	// This method can block until the processor is available for processing new batch,
	// and may block indefinitely.
	// It is recommended to use [Processor.MergeContext] instead.
	Merge(item T, merge MergeToBatchFn[B, T])
	// MergeAll add all items to the processor using merge function.
	// This method can block until the processor is available for processing new batch,
	// and may block indefinitely.
	// It is recommended to use [Processor.MergeAllContext] instead.
	MergeAll(items []T, merge MergeToBatchFn[B, T])
	// MergeContext add item to the processor using merge function.
	// If the context is canceled and the item is not added, then this method will return false.
	// The context passed in only control the put step, after item added to the processor,
	// the processing will not be canceled by this context.
	MergeContext(ctx context.Context, item T, merge MergeToBatchFn[B, T]) bool
	// MergeAllContext add all items to the processor using merge function.
	// If the context is canceled, then this method will return the number of items added to the processor.
	MergeAllContext(ctx context.Context, items []T, merge MergeToBatchFn[B, T]) int

	// Peek access the current batch using provided function.
	// This method can block until the processor is available.
	// It is recommended to use [Processor.PeekContext] instead.
	// This method does count as processing the batch, the batch will still be processed.
	Peek(reader ProcessBatchFn[B]) error
	// PeekContext access the current batch using provided function.
	// This method does count as processing the batch, the batch will still be processed.
	PeekContext(ctx context.Context, reader ProcessBatchFn[B]) error

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
	// Context can be used to provide deadline for this method.
	CloseContext(ctx context.Context) error
	// StopContext stop the processor.
	// This method does not process leftover batch.
	StopContext(ctx context.Context) error
	// DrainContext force process batch util the batch is empty.
	// This method may process the batch on caller thread.
	// Context can be used to provide deadline for this method.
	DrainContext(ctx context.Context) error
	// FlushContext force process the current batch.
	// This method may process the batch on caller thread.
	// Context can be used to provide deadline for this method.
	FlushContext(ctx context.Context) error
	// Flush force process the current batch.
	// This method may process the batch on caller thread.
	// It is recommended to use [Processor.FlushContext] instead.
	Flush()
}

// SliceProcessor alias for [Processor] that merge item into slices.
type SliceProcessor[T any] = Processor[T, []T]

// MapProcessor shorthand for [Processor] that merge item into maps.
type MapProcessor[K comparable, T any] = Processor[T, map[K]T]

// runningProcessor processor that is running and can process item.
type runningProcessor[T any, B any] struct {
	ProcessorSetup[T, B]
	runConfig[B]
	process ProcessBatchFn[B]

	// batch is the current batch of item.
	batch B
	// counter is the count of item in the batch.
	// By default, it is the number of requests put into the processor that has not been processed.
	// Can be customized using [WithBatchCounter].
	counter int64

	// notEmpty notify when the processor is not empty.
	// Used only for aggressive mode.
	notEmpty chan struct{}
	// full notify when the processor is full.
	full chan struct{}
	// blocked internal batch lock.
	blocked chan struct{}
	// closed notify when the processor is closed.
	closed chan struct{}
}

// NewProcessor create a ProcessorSetup using specified functions.
// See [ProcessorSetup.Configure] and [Option] for available configuration.
// The result [ProcessorSetup] is in setup state.
// Call [ProcessorSetup.Run] with a handler to create a [runningProcessor] that can accept item.
// It is recommended to set at least maxWait by [WithMaxWait] or maxItem by [WithMaxItem].
// By default, the processor operates similarly to aggressive mode, use Configure to change its behavior.
func NewProcessor[T any, B any](init InitBatchFn[B], merge MergeToBatchFn[B, T]) ProcessorSetup[T, B] {
	c := processorConfig{
		maxWait: 0,
		// Default unlimited for maxItem.
		maxItem: Unset,
	}
	return ProcessorSetup[T, B]{
		processorConfig: c,
		init:            init,
		merge:           merge,
	}
}

// Configure apply Option to this processor.
// Each Configure call creates a new processor.
func (p ProcessorSetup[T, B]) Configure(options ...Option) ProcessorSetup[T, B] {
	for i := range options {
		options[i](&p.processorConfig)
	}
	return p
}

// ItemCount return number of current item in processor.
// This method will block the processor for accurate counting.
// It is recommended to use [runningProcessor.ItemCountContext] instead.
func (p *runningProcessor[T, B]) ItemCount() int64 {
	cnt, _ := p.ItemCountContext(context.Background())
	return cnt
}

// ItemCountContext return number of current item in processor.
// If the context is canceled, then this method will return approximate item count and false.
func (p *runningProcessor[T, B]) ItemCountContext(ctx context.Context) (int64, bool) {
	select {
	case p.blocked <- struct{}{}:
	case <-ctx.Done():
		return p.counter, false
	}
	defer func() { <-p.blocked }()
	return p.counter, true
}

// ApproxItemCount return number of current item in processor.
// This method does not block, so the counter may not be accurate.
func (p *runningProcessor[T, B]) ApproxItemCount() int64 {
	return p.counter
}

func (p ProcessorSetup[T, B]) isAggressiveMode() bool {
	return p.aggressive || (p.maxWait == 0 && p.maxItem < 0)
}

// Run create a [Processor] that can accept item.
// Accept a [ProcessBatchFn] and a list of [RunOption].
func (p ProcessorSetup[T, B]) Run(process ProcessBatchFn[B], options ...RunOption[B]) Processor[T, B] {
	processor := &runningProcessor[T, B]{
		ProcessorSetup: p,
		process:        process,
		full:           make(chan struct{}),
		blocked:        make(chan struct{}, 1),
		closed:         make(chan struct{}),
	}

	for i := range options {
		options[i](&processor.runConfig)
	}

	// if errorHandlers is empty, then add a default logging handler.
	if !p.isDisableErrorLogging && len(processor.errorHandlers) == 0 {
		processor.errorHandlers = []RecoverBatchFn[B]{LoggingErrorHandler[B]}
	}

	if p.isAggressiveMode() {
		processor.maxWait = 0
		processor.isBlockWhileProcessing = false
		processor.notEmpty = make(chan struct{}, 1)
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
		if processor.notEmpty != nil {
			processor.continuousDispatch()
		} else {
			processor.waitUtilFullContinuousDispatch()
		}
		return processor
	}

	processor.timedDispatch()
	return processor
}

// RunOptions alias of [ProcessorSetup.Run] for migration from v1.
// Deprecated: to be removal, use [ProcessorSetup.Run] instead.
func (p ProcessorSetup[T, B]) RunOptions(process ProcessBatchFn[B], options ...RunOption[B]) Processor[T, B] {
	return p.Run(process, options...)
}

// continuousDispatch create a dispatcher routine that,
// when batch is empty, wait util it not empty,
// else process the remaining batch util it became empty.
func (p *runningProcessor[T, B]) continuousDispatch() {
	if p.notEmpty == nil {
		// Should never happen.
		panic("Empty channel is nil. This is a bug in the lib!")
	}
	go func() {
		for {
			select {
			case p.blocked <- struct{}{}:
				// if the batch is empty, then try to wait until non-empty.
				if p.counter == 0 {
					<-p.blocked
					select {
					case <-p.notEmpty:
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

// waitUtilFullContinuousDispatch create a dispatcher routine that,
// when batch is empty, wait util it full,
// else process the remaining batch util it became empty.
// maxItem must be specified by [WithMaxItem].
func (p *runningProcessor[T, B]) waitUtilFullContinuousDispatch() {
	go func() {
		for {
			select {
			case p.blocked <- struct{}{}:
				// if the batch is not empty, then process anyway.
				if p.counter > 0 {
					p.doProcessAndRelease(p.isBlockWhileProcessing)
					break
				}
				<-p.blocked
				// if the batch is empty then wait util full to process.
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
func (p *runningProcessor[T, B]) timedDispatch() {
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
// maxItem must be specified using [WithMaxItem].
func (p *runningProcessor[T, B]) waitUtilFullDispatch() {
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
func (p *runningProcessor[T, B]) IsDisabled() bool {
	return p.maxItem == 0
}

// Put add item to the processor.
// This method can block until the processor is available for processing new batch.
// It is recommended to use [runningProcessor.PutContext] instead.
func (p *runningProcessor[T, B]) Put(item T) {
	//nolint:staticcheck
	p.PutContext(nil, item)
}

// PutContext add item to the processor.
func (p *runningProcessor[T, B]) PutContext(ctx context.Context, item T) bool {
	return p.MergeContext(ctx, item, p.merge)
}

// Merge add item to the processor.
// This method can block until the processor is available for processing new batch.
// It is recommended to use [Processor.MergeContext] instead.
func (p *runningProcessor[T, B]) Merge(item T, merge MergeToBatchFn[B, T]) {
	//nolint:staticcheck
	p.MergeContext(nil, item, merge)
}

// MergeContext add item to the processor using merge function.
func (p *runningProcessor[T, B]) MergeContext(ctx context.Context, item T, merge MergeToBatchFn[B, T]) bool {
	if ctx != nil && ctx.Err() != nil {
		return false
	}

	if p.IsDisabled() {
		batch := merge(p.init(1), item)
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

	p.batch = merge(p.batch, item)
	if p.count != nil {
		p.counter = p.count(p.batch, p.counter)
	} else {
		p.counter++
	}
	if p.notEmpty != nil && p.counter > 0 {
		select {
		case p.notEmpty <- struct{}{}:
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

// Peek access the current batch using provided function.
// This method can block until the processor is available.
// It is recommended to use [Processor.PeekContext] instead.
// This method does count as processing the batch, the batch will still be processed.
func (p *runningProcessor[T, B]) Peek(reader ProcessBatchFn[B]) error {
	//nolint:staticcheck
	return p.PeekContext(nil, reader)
}

// PeekContext access the current batch using provided function.
// This method does count as processing the batch, the batch will still be processed.
func (p *runningProcessor[T, B]) PeekContext(ctx context.Context, reader ProcessBatchFn[B]) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}

	if p.IsDisabled() {
		return reader(p.init(1), 0)
	}

	// Select is slow, and most of the old codes are using Put without Context,
	// so we allow nil context to preserve performance.
	if ctx != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
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

	err := reader(p.batch, p.counter)
	<-p.blocked
	return err
}

// PutAll add all item to the processor.
// This method will block until all items were put into the processor.
// It is recommended to use [Processor.PutAllContext] instead.
func (p *runningProcessor[T, B]) PutAll(items []T) {
	//nolint:staticcheck
	p.PutAllContext(nil, items)
}

// PutAllContext add all items to the processor.
// If the context is canceled, then this method will return the number of items added to the processor.
// The processing order is the same as the input list,
// so the output can also be used to determine the next item to process if you want to retry or continue processing.
func (p *runningProcessor[T, B]) PutAllContext(ctx context.Context, items []T) int {
	return p.MergeAllContext(ctx, items, p.merge)
}

// MergeAll add all item to the processor using merge function.
// This method will block until all items were put into the processor.
// It is recommended to use [Processor.MergeAllContext] instead.
func (p *runningProcessor[T, B]) MergeAll(items []T, merge MergeToBatchFn[B, T]) {
	//nolint:staticcheck
	p.MergeAllContext(nil, items, merge)
}

// MergeAllContext add all items to the processor using merge function.
// If the context is canceled, then this method will return the number of items added to the processor.
// The processing order is the same as the input list,
// so the output can also be used to determine the next item to process if you want to retry or continue processing.
func (p *runningProcessor[T, B]) MergeAllContext(ctx context.Context, items []T, merge MergeToBatchFn[B, T]) int {
	if len(items) == 0 {
		return 0
	}
	if p.IsDisabled() {
		batch := p.init(int64(len(items)))
		for i := range items {
			batch = merge(batch, items[i])
		}
		p.doProcess(batch, 1)
		return len(items)
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

	ok := 0
	for ok < len(items) {
		if ctx != nil && ctx.Err() != nil {
			return ok
		}

		if ctx != nil {
			select {
			case <-ctx.Done():
				return ok
			case p.blocked <- struct{}{}:
			}
		} else {
			p.blocked <- struct{}{}
		}

		available := int64(len(items) - ok)
		if p.maxItem > -1 {
			available = min(p.maxItem-p.counter, available)
		}
		for i := int64(ok); i < int64(ok)+available; i++ {
			p.batch = merge(p.batch, items[i])
		}
		if p.count != nil {
			p.counter = p.count(p.batch, p.counter)
		} else {
			p.counter += available
		}
		ok += int(available)

		if p.notEmpty != nil && p.counter > 0 {
			select {
			case p.notEmpty <- struct{}{}:
				// notify that the batch is now not empty.
			default:
				// processing, no need to modify.
			}
		}
		if p.maxItem > -1 && p.counter >= p.maxItem {
			// Block util processed.
			p.full <- struct{}{}
			continue
		}
		<-p.blocked
	}
	return ok
}

// Close stop the processor.
// This method will process the leftover branch on caller thread.
// Return error if maxCloseWait passed.
// Timeout can be configured by [WithMaxCloseWait].
// See getCloseMaxWait for detail.
func (p *runningProcessor[T, B]) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), p.getCloseMaxWait())
	defer cancel()
	return p.CloseContext(ctx)
}

// CloseContext stop the processor.
// This method will process the leftover branch on caller thread.
// Context can be used to provide deadline for this method.
func (p *runningProcessor[T, B]) CloseContext(ctx context.Context) error {
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
func (p *runningProcessor[T, B]) StopContext(ctx context.Context) error {
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
func (p *runningProcessor[T, B]) DrainContext(ctx context.Context) error {
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
// Context can be used to provide deadline for this method.
func (p *runningProcessor[T, B]) FlushContext(ctx context.Context) error {
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
// It is recommended to use [Processor.FlushContext] instead.
func (p *runningProcessor[T, B]) Flush() {
	err := p.FlushContext(context.Background())
	if err != nil {
		// should never have happened.
		slog.Error("error flushing batch", slog.Any("err", err))
	}
}

func (p *runningProcessor[T, B]) getCloseMaxWait() time.Duration {
	if p.maxCloseWait > 0 {
		return p.maxCloseWait
	}
	// if maxWait is set, then wait double the time.
	if p.maxWait > 0 {
		return p.maxWait * 2
	}
	return 15 * time.Second
}

// doProcessAndRelease process the batch and release the lock.
func (p *runningProcessor[T, B]) doProcessAndRelease(block bool) {
	batch := p.batch
	counter := p.counter
	if block {
		defer func() {
			if r := recover(); r != nil {
				p.batch = p.init(p.maxItem)
				p.counter = 0
				<-p.blocked
				panic(r)
			}
		}()
		p.doProcessConcurrency(batch, counter)
		p.batch = p.init(p.maxItem)
		p.counter = 0
		// Release after processing.
		<-p.blocked
		return
	}
	p.batch = p.init(p.maxItem)
	p.counter = 0
	// Release before processing.
	<-p.blocked
	p.doProcessConcurrency(batch, counter)
}

// doProcessConcurrency process the batch with concurrency if enabled.
func (p *runningProcessor[T, B]) doProcessConcurrency(batch B, counter int64) {
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

// doAcquireThreadAndProcess acquire a thread and process the batch.
func (p *runningProcessor[T, B]) doAcquireThreadAndProcess(batch B, counter int64) {
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

// doProcess process the batch and passing any error to error handlers.
func (p *runningProcessor[T, B]) doProcess(batch B, counter int64) {
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

// doHandleError execute [RecoverBatchFn] chain.
func (p *runningProcessor[T, B]) doHandleError(batch B, counter int64, err error) {
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
