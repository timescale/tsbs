package targets

// Processor is a type that processes the work for a loading worker
type Processor interface {
	// Init does per-worker setup needed before receiving data
	Init(workerNum int, doLoad, hashWorkers bool)
	// ProcessBatch handles a single batch of data
	ProcessBatch(b Batch, doLoad bool) (metricCount, rowCount uint64)
}

// ProcessorCloser is a Processor that also needs to close or cleanup afterwards
type ProcessorCloser interface {
	Processor
	// Close cleans up after a Processor
	Close(doLoad bool)
}
