package load

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// DefaultBatchSize is the default size of batches to be inserted
	DefaultBatchSize = 10000
	defaultReadSize  = 4 << 20 // 4 MB
)

// Benchmark is an interface that represents the skeleton of a program
// needed to run an insert or load benchmark.
type Benchmark interface {
	Work(*sync.WaitGroup, int)
	Scan(batchSize int, limit int64, br *bufio.Reader) int64
	Close()
}

// CleaningBenchmark is an interface for programs that need to cleanup before
// printing the summary. It should be combined with Benchmark
type CleaningBenchmark interface {
	Benchmark
	Cleanup()
}

// BenchmarkRunner is responsible for initializing and storing common
// flags across all database systems and ultimately running a supplied Benchmark
type BenchmarkRunner struct {
	dbName          string
	batchSize       int
	workers         int
	limit           int64
	doLoad          bool
	doInit          bool
	reportingPeriod time.Duration
	filename        string // TODO implement file reading

	// non-flag fields
	br *bufio.Reader
}

var loader = &BenchmarkRunner{}

// GetBenchmarkRunner returns the singleton BenchmarkRunner for use in a benchmark program
// with a batch size of 10000
func GetBenchmarkRunner() *BenchmarkRunner {
	return GetBenchmarkRunnerWithBatchSize(DefaultBatchSize)
}

// GetBenchmarkRunnerWithBatchSize returns the singleton BenchmarkRunner for use in a benchmark program
// with a non-default batch size.
func GetBenchmarkRunnerWithBatchSize(batchSize int) *BenchmarkRunner {
	flag.StringVar(&loader.dbName, "db-name", "benchmark", "Name of database")

	flag.IntVar(&loader.batchSize, "batch-size", batchSize, "Number of items to batch together in a single insert")
	flag.IntVar(&loader.workers, "workers", 1, "Number of parallel clients inserting")
	flag.Int64Var(&loader.limit, "limit", -1, "Number of items to insert (default unlimited).")
	flag.BoolVar(&loader.doLoad, "do-load", true, "Whether to write data. Set this flag to false to check input read speed")
	flag.BoolVar(&loader.doInit, "do-init", true, "Whether to initialize the database. Disable on all but one box if running on a multi client box setup.")
	flag.DurationVar(&loader.reportingPeriod, "reporting-period", 10*time.Second, "Period to report write stats")

	return loader
}

// DatabaseName returns the value of the --db-name flag (name of the database to store data)
func (l *BenchmarkRunner) DatabaseName() string {
	return l.dbName
}

// DoLoad returns the value of the --do-load flag (whether to actually load or not)
func (l *BenchmarkRunner) DoLoad() bool {
	return l.doLoad
}

// DoInit returns the value of the --do-init flag (whether to actually initialize the DB or not)
func (l *BenchmarkRunner) DoInit() bool {
	return l.doInit
}

// NumWorkers returns the value of the --workers flag (how many parallel insert clients there are)
func (l *BenchmarkRunner) NumWorkers() int {
	return l.workers
}

// RunBenchmark takes in a Benchmark b, a bufio.Reader br, and holders for number of metrics and rows
// and uses those to run the load benchmark
func (l *BenchmarkRunner) RunBenchmark(b Benchmark, metricCount, rowCount *uint64) {
	l.br = l.GetBufferedReader()
	var wg sync.WaitGroup

	for i := 0; i < l.workers; i++ {
		wg.Add(1)
		b.Work(&wg, i)
	}

	start := time.Now()
	l.scan(b, metricCount, rowCount)
	switch c := b.(type) {
	case CleaningBenchmark:
		c.Cleanup()
	}

	b.Close()
	wg.Wait()
	end := time.Now()

	summary(end.Sub(start), l.workers, metricCount, rowCount)
}

// GetBufferedReader returns the buffered Reader that should be used by the loader
func (l *BenchmarkRunner) GetBufferedReader() *bufio.Reader {
	if l.br == nil {
		if len(l.filename) > 0 {
			l.br = nil // TODO - Support reading from files
		} else {
			l.br = bufio.NewReaderSize(os.Stdin, defaultReadSize)
		}
	}
	return l.br
}

// scan launches any needed reporting mechanism and proceeds to scan input data
// to distribute to workers
func (l *BenchmarkRunner) scan(b Benchmark, metricCount, rowCount *uint64) int64 {
	if l.reportingPeriod.Nanoseconds() > 0 {
		go report(l.reportingPeriod, metricCount, rowCount)
	}
	return b.Scan(l.batchSize, l.limit, l.br)
}

// summary prints the summary of statistics from loading
func summary(took time.Duration, workers int, metricCount, rowCount *uint64) {
	metricRate := float64(*metricCount) / float64(took.Seconds())
	fmt.Println("\nSummary:")
	fmt.Printf("loaded %d metrics in %0.3fsec with %d workers (mean rate %0.2f metrics/sec)\n", *metricCount, took.Seconds(), workers, metricRate)
	if rowCount != nil {
		rowRate := float64(*rowCount) / float64(took.Seconds())
		fmt.Printf("loaded %d rows in %0.3fsec with %d workers (mean rate %0.2f rows/sec)\n", *rowCount, took.Seconds(), workers, rowRate)
	}
}

// report handles periodic reporting of loading stats
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
