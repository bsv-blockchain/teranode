package subtreeprocessor

type Options func(*SubtreeProcessor)

func WithBatcherSize(size int) Options {
	return func(sp *SubtreeProcessor) {
		sp.batcher = NewTxIDAndFeeBatch(size)
	}
}
