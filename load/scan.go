package load

import (
	"bufio"
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
	benchmark    Benchmark
	workersGroup sync.WaitGroup

	start time.Time
	end   time.Time
}

// NewDataReader returns a new DataReader for the given parameters
func NewDataReader(workers int, b Benchmark) *DataReader {
	d := &DataReader{
		itemsCount: 0,
		bytesCount: 0,
		inputDone:  make(chan struct{}),
		benchmark:  b,
	}

	for i := 0; i < workers; i++ {
		d.workersGroup.Add(1)
		b.Work(&d.workersGroup, i)
	}

	return d
}

// Start begins the reading of input data and sending it to workers for loading.
// If a non-0 reportingPeriod is supplied, a CSV of metrics is reported at
// reportingPeriod intervals.
// metricCount is a pointer to an int64 that contains the number of metrics counted since the program began
// rowCount is a (optional) pointer to an in64 that contains the number of rows counted since the program began
func (d *DataReader) Start(br *bufio.Reader, batchSize int, reportPeriod time.Duration, metricCount, rowCount *uint64) int64 {
	d.start = time.Now()
	if reportPeriod.Nanoseconds() > 0 {
		go report(reportPeriod, metricCount, rowCount)
	}
	itemsRead := d.benchmark.Scan(batchSize, br)
	close(d.inputDone)

	<-d.inputDone
	d.benchmark.Close()
	d.workersGroup.Wait()
	d.end = time.Now()

	return itemsRead
}

// Took returns the time.Duration of how long the scanning and processing input took
func (d *DataReader) Took() time.Duration {
	return d.end.Sub(d.start)
}

// Summary outputs the summary lines for a given DataReader run
func (d *DataReader) Summary(workers int, metricCount, rowCount *uint64) {
	took := d.Took()

	metricRate := float64(*metricCount) / float64(took.Seconds())
	fmt.Println("\nSummary:")
	fmt.Printf("loaded %d metrics in %0.3fsec with %d workers (mean rate %0.3f values/sec)\n", *metricCount, took.Seconds(), workers, metricRate)
	if rowCount != nil {
		rowRate := float64(*rowCount) / float64(took.Seconds())
		fmt.Printf("loaded %d rows in %0.3fsec with %d workers (mean rate %0.3f/sec)\n", *rowCount, took.Seconds(), workers, rowRate)
	}
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
			fmt.Printf("%d,%0.3f,%E,%0.3f,%0.3f,%E,%0.3f\n", now.Unix(), colrate, float64(cCount), overallColRate, rowrate, float64(rCount), overallRowRate)
		} else {
			fmt.Printf("%d,%0.3f,%E,%0.3f,-,-,-\n", now.Unix(), colrate, float64(cCount), overallColRate)
		}

		prevColCount = cCount
		prevRowCount = rCount
		prevTime = now
	}
}
