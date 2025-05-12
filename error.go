package batch

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
