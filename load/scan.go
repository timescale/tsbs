package load

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// DataReader is a framework for controlling reading of input data
type DataReader struct {
	itemsCount   int64
	bytesCount   int64
	inputDone    chan struct{}
	scanFn       func() (int64, int64)
	workersGroup sync.WaitGroup

	start time.Time
	end   time.Time
}

// NewDataReader returns a new DataReader for the given parameters
func NewDataReader(workers int, workerFn func(*sync.WaitGroup, int), scanFn func() (int64, int64)) *DataReader {
	d := &DataReader{
		itemsCount: 0,
		bytesCount: 0,
		inputDone:  make(chan struct{}),
		scanFn:     scanFn,
	}

	for i := 0; i < workers; i++ {
		d.workersGroup.Add(1)
		workerFn(&d.workersGroup, i)
	}

	return d
}

// Start begins the reading of input data and sending it to workers for loading.
// If a non-0 reportingPeriod is supplied, a CSV of metrics is reported at
// reportingPeriod intervals.
// closeFn is a function for cleaning up any channels related to batching and reading.
// metricCount is a pointer to an int64 that contains the number of metrics counted since the program began
// rowCount is a (optional) pointer to an in64 that contains the number of rows counted since the program began
func (d *DataReader) Start(reportPeriod time.Duration, closeFn func(), metricCount, rowCount *uint64) (int64, int64) {
	d.start = time.Now()
	if reportPeriod.Nanoseconds() > 0 {
		go report(reportPeriod, metricCount, rowCount)
	}
	itemsRead, bytesRead := d.scanFn()
	close(d.inputDone)

	<-d.inputDone
	closeFn()
	d.workersGroup.Wait()
	d.end = time.Now()

	return itemsRead, bytesRead
}

// Took returns the time.Duration of how long the scanning and processing input took
func (d *DataReader) Took() time.Duration {
	return d.end.Sub(d.start)
}

func report(period time.Duration, metricCount, rowCount *uint64) {
	start := time.Now()
	prevTime := start
	prevColCount := uint64(0)
	prevRowCount := uint64(0)

	rCount := uint64(0)
	fmt.Printf("time,per. metric/s,metric total,overall metric/s,per. row/s,row total,overall row/s\n")
	for now := range time.NewTicker(period).C {
		cCount := atomic.LoadUint64(metricCount)
		if rowCount != nil {
			rCount = atomic.LoadUint64(rowCount)
		}

		sinceStart := now.Sub(start)
		took := now.Sub(prevTime)
		colrate := float64(cCount-prevColCount) / float64(took.Seconds())
		overallColRate := float64(cCount) / float64(sinceStart.Seconds())
		if rowCount != nil {
			rowrate := float64(rCount-prevRowCount) / float64(took.Seconds())
			overallRowRate := float64(rCount) / float64(sinceStart.Seconds())
			fmt.Printf("%d,%f,%E,%f,%f,%E,%f\n", now.Unix(), colrate, float64(cCount), overallColRate, rowrate, float64(rCount), overallRowRate)
		} else {
			fmt.Printf("%d,%f,%E,%f,-,-,-\n", now.Unix(), colrate, float64(cCount), overallColRate)
		}

		prevColCount = cCount
		prevRowCount = rCount
		prevTime = now
	}
}
