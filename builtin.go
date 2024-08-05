package batch

// InitSlice is InitBatchFn that allocate a slice.
func InitSlice[T any](i int64) []T {
	if i < 0 {
		return make([]T, 0)
	}
	if i == 0 {
		return make([]T, 1)
	}
	return make([]T, 0, i)
}

// AddToSlice is MergeToBatchFn that add item to a slice.
func AddToSlice[T any](b []T, item T) []T {
	return append(b, item)
}

// InitMap is InitBatchFn that allocate a map.
func InitMap[K comparable, V any](i int64) map[K]V {
	if i == 0 {
		return make(map[K]V, 1)
	}
	return make(map[K]V)
}

type Extractor[T any, V any] func(T) V
type Combine[T any] func(T, T) T

// AddToMapUsing create MergeToBatchFn that add item to map using key and value Extractor.
func AddToMapUsing[T any, K comparable, V any](keyExtractor Extractor[T, K], valueExtractor Extractor[T, V]) MergeToBatchFn[map[K]V, T] {
	return func(m map[K]V, t T) map[K]V {
		key := keyExtractor(t)
		value := valueExtractor(t)
		m[key] = value
		return m
	}
}

// AddSelfToMapUsing create MergeToBatchFn that add self as item to map using key Extractor.
func AddSelfToMapUsing[T any, K comparable](keyExtractor Extractor[T, K]) MergeToBatchFn[map[K]T, T] {
	return func(m map[K]T, t T) map[K]T {
		key := keyExtractor(t)
		m[key] = t
		return m
	}
}

// MergeToMapUsing create MergeToBatchFn that add item to map using key and value Extractor and apply Combine if key duplicated.
// The original value will be passed as 1st parameter to Combine function.
func MergeToMapUsing[T any, K comparable, V any](keyExtractor Extractor[T, K], valueExtractor Extractor[T, V], combiner Combine[V]) MergeToBatchFn[map[K]V, T] {
	return func(m map[K]V, t T) map[K]V {
		key := keyExtractor(t)
		value := valueExtractor(t)
		if v, ok := m[key]; ok {
			m[key] = combiner(v, value)
			return m
		}
		m[key] = value
		return m
	}
}

// MergeSelfToMapUsing create MergeToBatchFn that add self as item to map using key Extractor and apply Combine if key duplicated.
// The original value will be passed as 1st parameter to Combine function.
func MergeSelfToMapUsing[T any, K comparable](keyExtractor Extractor[T, K], combiner Combine[T]) MergeToBatchFn[map[K]T, T] {
	return func(m map[K]T, t T) map[K]T {
		key := keyExtractor(t)
		if v, ok := m[key]; ok {
			m[key] = combiner(v, t)
			return m
		}
		m[key] = t
		return m
	}
}

// InitChan is InitBatchFn that allocate a channel.
// this should not be used with unbounded processor (maxItem < 0).
func InitChan[T any](i int64) chan T {
	if i < 0 {
		panic("cannot use unbounded processor with channel batch")
	}
	if i == 0 {
		return make(chan T, 1)
	}
	return make(chan T, i)
}

// InitType is InitBatchFn that allocate a type T.
func InitType[T any](_ int64) T {
	t := new(T)
	return *t
}

// SplitSliceEqually create a SplitBatchFn that split a slice into multiple equal chuck.
func SplitSliceEqually[T any, I Size](numberOfChunk I) SplitBatchFn[[]T] {
	return func(b []T, _ int64) [][]T {
		batches := make([][]T, numberOfChunk)
		for i := 0; i < len(b); i++ {
			bucket := int64(i) % int64(numberOfChunk)
			batches[bucket] = append(batches[bucket], b[i])
		}
		return batches
	}
}

// SplitSliceSizeLimit create a SplitBatchFn that split a slice into multiple chuck of limited size.
func SplitSliceSizeLimit[T any, I Size](maxSizeOfChunk I) SplitBatchFn[[]T] {
	return func(b []T, i int64) [][]T {
		size := i / int64(maxSizeOfChunk)
		if i%int64(maxSizeOfChunk) != 0 {
			size++
		}
		batches := make([][]T, size)
		index := 0
		for batchI := 0; batchI < len(batches); batchI++ {
			batch := batches[batchI]
			for ; index < len(b); index++ {
				batch = append(batch, b[index])
			}
			batches[batchI] = batch
		}
		return batches
	}
}
