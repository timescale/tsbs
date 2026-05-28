package load

import (
	"github.com/timescale/tsbs/pkg/targets"
	"sync"
	"sync/atomic"
	"time"
)

type noFlowBenchmarkRunner struct {
	CommonBenchmarkRunner
}

func (l *noFlowBenchmarkRunner) RunBenchmark(b targets.Benchmark) {
	wg, start, cleanupFn := l.preRun(b)

	var numChannels uint
	if l.HashWorkers {
		numChannels = l.Workers
	} else {
		numChannels = 1
	}
	channels := l.createChannels(numChannels, l.ChannelCapacity)

	// Launch all worker processes in background
	for i := uint(0); i < l.Workers; i++ {
		go l.work(b, wg, channels[i%numChannels], i)
	}
	// Start scan process - actual data read process
	scanWithoutFlowControl(b.GetDataSource(), b.GetPointIndexer(numChannels), b.GetBatchFactory(), channels, l.BatchSize, l.Limit)
	for _, c := range channels {
		close(c)
	}
	cleanupFn()
	l.postRun(wg, start)
}

// createChannels create channels from which workers would receive tasks
// One channel per worker
func (l *noFlowBenchmarkRunner) createChannels(numChannels, capacity uint) []chan targets.Batch {
	// Result - channels to be created
	channels := make([]chan targets.Batch, numChannels)
	for i := uint(0); i < numChannels; i++ {
		channels[i] = make(chan targets.Batch, capacity)
	}
	return channels
}

// work is the processing function for each worker in the loader
func (l *noFlowBenchmarkRunner) work(b targets.Benchmark, wg *sync.WaitGroup, c <-chan targets.Batch, workerNum uint) {
	// Prepare processor
	proc := b.GetProcessor()
	proc.Init(int(workerNum), l.DoLoad, l.HashWorkers)

	// Process batches coming from the incoming queue (c)
	for batch := range c {
		startedWorkAt := time.Now()
		metricCnt, rowCnt := proc.ProcessBatch(batch, l.DoLoad)
		atomic.AddUint64(&l.metricCnt, metricCnt)
		atomic.AddUint64(&l.rowCnt, rowCnt)
		l.timeToSleep(workerNum, startedWorkAt)
	}

	// Close proc if necessary
	switch c := proc.(type) {
	case targets.ProcessorCloser:
		c.Close(l.DoLoad)
	}

	wg.Done()
}
