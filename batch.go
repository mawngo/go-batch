package batch

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

var _ IProcessor[any, any] = (*Processor[any, any])(nil)

// ProcessorSetup is a batch processor in the setup phase (not running).
// You cannot put an item into this processor; use [ProcessorSetup.Run] to create a [Processor] that can accept items.
// See [Option] for available options.
type ProcessorSetup[T any, B any] struct {
	processorConfig
	mergeFn MergeToBatchFn[B, T]
	initFn  InitBatchFn[B]
}

// IProcessor provides common methods of a [Processor].
type IProcessor[T any, B any] interface {
	// Put adds an item to the processor.
	// If the context is canceled and the item is not added, then this method will return false.
	// The context passed in only controls the put step.
	// After the item is added to the processor, this context will not cancel the processing.
	Put(ctx context.Context, item T) bool
	// PutAll adds all items to the processor.
	// If the context is canceled, then this method will return the number of items added to the processor.
	PutAll(ctx context.Context, items []T) int

	// Merge adds an item to the processor using the merge function.
	// If the context is canceled and the item is not added, then this method will return false.
	// The context passed in only controls the put step.
	// After the item is added to the processor, this context will not cancel the processing.
	Merge(ctx context.Context, item T, merge MergeToBatchFn[B, T]) bool
	// MergeAll adds all items to the processor using a merge function.
	// If the context is canceled, then this method will return the number of items added to the processor.
	MergeAll(ctx context.Context, items []T, merge MergeToBatchFn[B, T]) int

	// ApproxItemCount returns the number of current items in the processor, approximately.
	ApproxItemCount() int64
	// ItemCount returns the number of current items in the processor.
	// If the context is canceled, then this method will return an approximate item count and false.
	ItemCount(ctx context.Context) (int64, bool)
	// Close stops the processor.
	// This method may process the left-over batch on the caller thread.
	// Context can be used to provide a deadline for this method.
	Close(ctx context.Context) error
	// Stop the processor.
	// This method does not process leftover batch.
	Stop(ctx context.Context) error
	// Drain forces processing batches until the batch is empty.
	// This method may process the batch on the caller thread.
	// Context can be used to provide a deadline for this method.
	Drain(ctx context.Context) error
	// Flush forces processing of the current batch.
	// This method may process the batch on the caller thread.
	// Context can be used to provide a deadline for this method.
	Flush(ctx context.Context) error
}

// Processor is a processor running and can process items.
type Processor[T any, B any] struct {
	ProcessorSetup[T, B]
	runConfig[B]
	process ProcessBatchFn[B]

	// batch is the current batch of items.
	batch B
	// counter is the count of items in the batch.
	// By default, it is the number of requests put into the processor that have not been processed.
	// Can be customized using [WithBatchCounter].
	counter int64

	// notEmpty notifies when the processor is not empty.
	// Used only for aggressive mode.
	notEmpty chan struct{}
	// full notifies when the processor is full.
	full chan struct{}
	// blocked is the internal batch lock.
	blocked chan struct{}
	// closing notifies when the processor is closing.
	closing chan struct{}
	closed  chan struct{}
}

// NewProcessor creates a ProcessorSetup using the specified functions.
// See [ProcessorSetup.Configure] and [Option] for available configuration.
// The resulting [ProcessorSetup] is in the setup state.
// Call [ProcessorSetup.Run] with a handler to create a [Processor] that can accept items.
//
// By default, the max wait time is 0 (no-wait) and the max item is [Unset] (unlimited), which makes
// the processor operate similarly to aggressive mode.
// Use [ProcessorSetup.Configure] to change its behavior. It is recommended to set at least maxWait by [WithMaxWait]
// or maxItem by [WithMaxItem] and BOTH if you enabled concurrency.
func NewProcessor[T any, B any](initFn InitBatchFn[B], mergeFn MergeToBatchFn[B, T]) ProcessorSetup[T, B] {
	c := processorConfig{
		maxWait: 0,
		// Default unlimited for maxItem.
		maxItem: Unset,
	}
	return ProcessorSetup[T, B]{
		processorConfig: c,
		initFn:          initFn,
		mergeFn:         mergeFn,
	}
}

// Configure applies [Option] to this processor setup.
func (p ProcessorSetup[T, B]) Configure(options ...Option) ProcessorSetup[T, B] {
	for i := range options {
		options[i](&p.processorConfig)
	}
	return p
}

// ItemCount returns the number of current items in the processor.
// If the context is canceled, then this method will return an approximate item count and false.
func (p *Processor[T, B]) ItemCount(ctx context.Context) (int64, bool) {
	select {
	case p.blocked <- struct{}{}:
	case <-ctx.Done():
		return p.counter, false
	case <-p.closed:
		return p.counter, false
	}
	defer func() { <-p.blocked }()
	return p.counter, true
}

// ApproxItemCount returns the number of current items in the processor.
// This method does not block, so the counter may not be accurate.
func (p *Processor[T, B]) ApproxItemCount() int64 {
	return p.counter
}

func (p ProcessorSetup[T, B]) isAggressiveMode() bool {
	return p.aggressive || (p.maxWait == 0 && p.maxItem < 0)
}

// Run creates a [Processor] that can accept items.
// Accepts a [ProcessBatchFn] and a list of [RunOption].
func (p ProcessorSetup[T, B]) Run(process ProcessBatchFn[B], options ...RunOption[B]) *Processor[T, B] {
	processor := &Processor[T, B]{
		ProcessorSetup: p,
		process:        process,
		full:           make(chan struct{}),
		blocked:        make(chan struct{}, 1),
		closing:        make(chan struct{}),
		closed:         make(chan struct{}),
	}

	for i := range options {
		options[i](&processor.runConfig)
	}
	// Always add a default error logging handler.
	if !p.isDisableErrorLogging {
		processor.errorHandlers = append(processor.errorHandlers, LoggingErrorHandler[B])
	}

	if p.isAggressiveMode() {
		processor.maxWait = 0
		processor.isBlockWhileProcessing = false
		processor.notEmpty = make(chan struct{}, 1)
	}

	processor.batch = p.initFn(p.maxItem)
	if processor.IsDisabled() {
		return processor
	}

	if p.maxWait < 0 && p.maxItem >= 0 {
		processor.waitUntilFullDispatch()
		return processor
	}

	if p.maxWait == 0 {
		if processor.notEmpty != nil {
			processor.continuousDispatch()
		} else {
			processor.waitUntilFullContinuousDispatch()
		}
		return processor
	}

	processor.timedDispatch()
	return processor
}

// continuousDispatch creates a dispatcher routine that,
// when the batch is empty, waits until it is not empty,
// otherwise processes the remaining batch until it becomes empty.
func (p *Processor[T, B]) continuousDispatch() {
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
						case p.closing <- struct{}{}:
							return
						}
					case <-p.full:
						p.doProcessAndRelease(p.isBlockWhileProcessing)
					case p.closing <- struct{}{}:
						return
					}
					break
				}
				p.doProcessAndRelease(p.isBlockWhileProcessing)
			case <-p.full:
				p.doProcessAndRelease(p.isBlockWhileProcessing)
			case p.closing <- struct{}{}:
				return
			}
		}
	}()
}

// waitUntilFullContinuousDispatch creates a dispatcher routine that,
// when the batch is empty, waits until it is full,
// otherwise processes the remaining batch until it becomes empty.
// maxItem must be specified by [WithMaxItem].
func (p *Processor[T, B]) waitUntilFullContinuousDispatch() {
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
				// if the batch is empty, then wait until full to process.
				select {
				case <-p.full:
					p.doProcessAndRelease(p.isBlockWhileProcessing)
				case p.closing <- struct{}{}:
					return
				}
			case <-p.full:
				p.doProcessAndRelease(p.isBlockWhileProcessing)
			case p.closing <- struct{}{}:
				return
			}
		}
	}()
}

// timedDispatch creates a dispatcher routine that waits until the batch is full or at least maxWait has elapsed.
// When maxWait has passed and the batch is empty, it will reset the timer to avoid processing only one item.
func (p *Processor[T, B]) timedDispatch() {
	go func() {
		for {
			timer := time.NewTimer(p.maxWait)
			select {
			case <-timer.C:
				select {
				case p.blocked <- struct{}{}:
					// if empty, then reset the timer.
					// this avoids the first item to be processed immediately after a long wait.
					if p.counter == 0 && !p.isHardMaxWait {
						<-p.blocked
						break
					}
					p.doProcessAndRelease(p.isBlockWhileProcessing)
				case <-p.full:
					p.doProcessAndRelease(p.isBlockWhileProcessing)
				case p.closing <- struct{}{}:
					return
				}
			case <-p.full:
				if !timer.Stop() {
					<-timer.C
				}
				p.doProcessAndRelease(p.isBlockWhileProcessing)
			case p.closing <- struct{}{}:
				if !timer.Stop() {
					<-timer.C
				}
				return
			}
		}
	}()
}

// waitUntilFullDispatch creates a dispatcher routine that waits until the batch is full.
// maxItem must be specified using [WithMaxItem].
func (p *Processor[T, B]) waitUntilFullDispatch() {
	go func() {
		for {
			select {
			case <-p.full:
				p.doProcessAndRelease(p.isBlockWhileProcessing)
			case p.closing <- struct{}{}:
				return
			}
		}
	}()
}

// IsDisabled returns whether the processor is disabled.
// A disabled processor won't do batching; instead, the process will be executed on the caller.
// All other settings are ignored when the processor is disabled, including the custom counter.
func (p *Processor[T, B]) IsDisabled() bool {
	return p.maxItem == 0
}

// Put adds an item to the processor.
func (p *Processor[T, B]) Put(ctx context.Context, item T) bool {
	return p.Merge(ctx, item, p.mergeFn)
}

// Merge adds an item to the processor using a merge function.
func (p *Processor[T, B]) Merge(ctx context.Context, item T, merge MergeToBatchFn[B, T]) bool {
	if ctx.Err() != nil {
		return false
	}

	if p.IsDisabled() {
		batch := merge(p.initFn(1), item)
		p.doProcess(batch, 1)
		return true
	}

	select {
	case <-ctx.Done():
		return false
	case <-p.closed:
		return false
	case p.blocked <- struct{}{}:
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
	if p.countFn != nil {
		p.counter = p.countFn(p.batch)
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
		select {
		case p.full <- struct{}{}:
			// Block until processed.
		case <-p.closed:
			<-p.blocked
		case <-ctx.Done():
			// The user canceled the context. They don't want to wait.
			// We still need to notify the full channel, so we offload it to a goroutine.
			// There will be no more than 1 extra goroutine at a time, because we still held the lock here.
			go func() {
				select {
				case <-p.closed:
					<-p.blocked
				case p.full <- struct{}{}:
				}
			}()
		}
		return true
	}
	<-p.blocked
	return true
}

// PutAll adds all items to the processor.
// If the context is canceled, then this method will return the number of items added to the processor.
// The processing order is the same as the input list,
// so the output can also be used to determine the next item to process if you want to retry or continue processing.
func (p *Processor[T, B]) PutAll(ctx context.Context, items []T) int {
	return p.MergeAll(ctx, items, p.mergeFn)
}

// MergeAll adds all items to the processor using a merge function.
// If the context is canceled, then this method will return the number of items added to the processor.
// The processing order is the same as the input list,
// so the output can also be used to determine the next item to process if you want to retry or continue processing.
func (p *Processor[T, B]) MergeAll(ctx context.Context, items []T, merge MergeToBatchFn[B, T]) int {
	if len(items) == 0 {
		return 0
	}
	if p.IsDisabled() {
		count := int64(len(items))
		batch := p.initFn(count)
		for i := range items {
			batch = merge(batch, items[i])
		}
		p.doProcess(batch, count)
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
		if ctx.Err() != nil {
			return ok
		}

		select {
		case <-ctx.Done():
			return ok
		case <-p.closed:
			return ok
		case p.blocked <- struct{}{}:
		}

		for {
			left := int64(len(items) - ok)
			if p.maxItem > -1 {
				left = min(p.maxItem-p.counter, left)
			}
			if left <= 0 {
				break
			}

			p.batch = merge(p.batch, items[ok])
			if p.countFn != nil {
				p.counter = p.countFn(p.batch)
			} else {
				p.counter++
			}
			ok++
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
			select {
			case p.full <- struct{}{}:
				// Block until processed.
				continue
			case <-p.closed:
				<-p.blocked
				return ok
			case <-ctx.Done():
				// The user canceled the context. They don't want to wait.
				// We still need to notify the full channel, so we offload it to a goroutine.
				// There will be no more than 1 extra goroutine at a time, because we still held the lock here.
				go func() {
					select {
					case <-p.closed:
						<-p.blocked
					case p.full <- struct{}{}:
					}
				}()
				return ok
			}
		}
		<-p.blocked
	}
	return ok
}

// Close stops the processor.
// This method will process the leftover batch on the caller thread.
//
// Context can be used to provide a deadline for this method.
// Context does not affect a batch already being processed.
func (p *Processor[T, B]) Close(ctx context.Context) error {
	if p.IsDisabled() {
		return nil
	}

	if ctx == nil {
		// Support for legacy close without context.
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), max(p.maxWait*2, 15*time.Second))
		defer cancel()
	}

	select {
	case _, cleanup := <-p.closing:
		if cleanup {
			close(p.closing)
		}
		slog.Debug("waiting for leftover batch to finish")
		if err := p.Drain(ctx); err != nil {
			if cleanup {
				close(p.closed)
				p.blocked <- struct{}{}
			}
			return err
		}
		if cleanup {
			close(p.closed)
			// Acquire the lock, so every method after this will run on the closed path.
			p.blocked <- struct{}{}
			if p.concurrentLimiter != nil {
				slog.Debug("waiting for concurrent workers to finish")
				if err := p.concurrentLimiter.Acquire(ctx, p.concurrentLimit); err != nil {
					return err
				}
				p.concurrentLimiter.Release(p.concurrentLimit)
			}
		}
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

// Stop the processor.
// This method does not process leftover batch.
func (p *Processor[T, B]) Stop(ctx context.Context) error {
	if p.IsDisabled() {
		return nil
	}
	select {
	case _, cleanup := <-p.closing:
		if cleanup {
			close(p.closing)
			close(p.closed)
			// Acquire the lock, so every method after this will run on the closed path.
			p.blocked <- struct{}{}
			if p.concurrentLimiter != nil {
				slog.Debug("waiting for concurrent workers to finish")
				if err := p.concurrentLimiter.Acquire(ctx, p.concurrentLimit); err != nil {
					return err
				}
				p.concurrentLimiter.Release(p.concurrentLimit)
			}
		}
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

// Drain forces processing batches until there is no leftover batch.
// This method always processes the batch on the caller thread.
// ctx can be used to provide a deadline for this method.
func (p *Processor[T, B]) Drain(ctx context.Context) error {
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
			p.batch = p.initFn(p.maxItem)
			p.counter = 0
			<-p.blocked
			p.doProcessConcurrency(batch, counter)
		case <-ctx.Done():
			return ctx.Err()
		case <-p.closed:
			return nil
		}
	}
	return nil
}

// Flush forces processing of the current batch.
// This method may process the batch on the caller thread, depending on concurrent and block settings.
// Context can be used to provide a deadline for this method.
func (p *Processor[T, B]) Flush(ctx context.Context) error {
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
		return ctx.Err()
	case <-p.closed:
		return nil
	}
	return nil
}

// doProcessAndRelease processes the batch and releases the lock.
func (p *Processor[T, B]) doProcessAndRelease(block bool) {
	batch := p.batch
	counter := p.counter
	if block {
		defer func() {
			if r := recover(); r != nil {
				p.batch = p.initFn(p.maxItem)
				p.counter = 0
				<-p.blocked
				panic(r)
			}
		}()
		p.doProcessConcurrency(batch, counter)
		p.batch = p.initFn(p.maxItem)
		p.counter = 0
		// Release after processing.
		<-p.blocked
		return
	}
	p.batch = p.initFn(p.maxItem)
	p.counter = 0
	// Release before processing.
	<-p.blocked
	p.doProcessConcurrency(batch, counter)
}

// doProcessConcurrency processes the batch with concurrency if enabled.
func (p *Processor[T, B]) doProcessConcurrency(batch B, counter int64) {
	if p.concurrentLimiter == nil {
		p.doProcess(batch, counter)
		return
	}

	if p.splitFn == nil {
		p.doAcquireThreadAndProcess(batch, counter)
		return
	}

	batches := p.splitFn(batch, counter)
	for i := range batches {
		p.doAcquireThreadAndProcess(batches[i], counter)
	}
}

// doAcquireThreadAndProcess acquires a thread and processes the batch.
func (p *Processor[T, B]) doAcquireThreadAndProcess(batch B, counter int64) {
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

// doProcess processes the batch and passes any error to the error handlers.
func (p *Processor[T, B]) doProcess(batch B, counter int64) {
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

// doHandleError executes the [RecoverBatchFn] chain.
func (p *Processor[T, B]) doHandleError(batch B, counter int64, err error) {
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
