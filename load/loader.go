package load

import (
	"bufio"
	"flag"
	"fmt"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// DefaultBatchSize is the default size of batches to be inserted
	defaultBatchSize = 10000
	defaultReadSize  = 4 << 20 // 4 MB

	// WorkerPerQueue is the value to have each worker have its own queue of batches
	WorkerPerQueue = 0
	// SingleQueue is the value to have only a single shared queue of work for all workers
	SingleQueue = 1

	errDBExistsFmt = "database \"%s\" exists: aborting."
)

// change for more useful testing
var printFn = fmt.Printf

// Benchmark is an interface that represents the skeleton of a program
// needed to run an insert or load benchmark.
type Benchmark interface {
	// GetPointDecoder returns the PointDecoder to use for this Benchmark
	GetPointDecoder(br *bufio.Reader) PointDecoder
	// GetBatchFactory returns the BatchFactory to use for this Benchmark
	GetBatchFactory() BatchFactory
	// GetPointIndexer returns the PointIndexer to use for this Benchmark
	GetPointIndexer(maxPartitions uint) PointIndexer
	// GetProcessor returns the Processor to use for this Benchmark
	GetProcessor() Processor

	GetDBCreator() DBCreator
}

// BenchmarkRunner is responsible for initializing and storing common
// flags across all database systems and ultimately running a supplied Benchmark
type BenchmarkRunner struct {
	dbName          string
	batchSize       uint
	workers         uint
	limit           uint64
	doLoad          bool
	doCreateDB      bool
	doAbortOnExist  bool
	reportingPeriod time.Duration
	filename        string // TODO implement file reading

	// non-flag fields
	br        *bufio.Reader
	metricCnt uint64
	rowCnt    uint64
}

var loader = &BenchmarkRunner{}

// GetBenchmarkRunner returns the singleton BenchmarkRunner for use in a benchmark program
// with a batch size of 10000
func GetBenchmarkRunner() *BenchmarkRunner {
	return GetBenchmarkRunnerWithBatchSize(defaultBatchSize)
}

// GetBenchmarkRunnerWithBatchSize returns the singleton BenchmarkRunner for use in a benchmark program
// with a non-default batch size.
func GetBenchmarkRunnerWithBatchSize(batchSize uint) *BenchmarkRunner {
	flag.StringVar(&loader.dbName, "db-name", "benchmark", "Name of database")

	flag.UintVar(&loader.batchSize, "batch-size", batchSize, "Number of items to batch together in a single insert")
	flag.UintVar(&loader.workers, "workers", 1, "Number of parallel clients inserting")
	flag.Uint64Var(&loader.limit, "limit", 0, "Number of items to insert (0 = all of them).")
	flag.BoolVar(&loader.doLoad, "do-load", true, "Whether to write data. Set this flag to false to check input read speed.")
	flag.BoolVar(&loader.doCreateDB, "do-create-db", true, "Whether to create the database. Disable on all but one client if running on a multi client setup.")
	flag.BoolVar(&loader.doAbortOnExist, "do-abort-on-exist", false, "Whether to abort if a database with the given name already exists.")
	flag.DurationVar(&loader.reportingPeriod, "reporting-period", 10*time.Second, "Period to report write stats")

	return loader
}

// DatabaseName returns the value of the --db-name flag (name of the database to store data)
func (l *BenchmarkRunner) DatabaseName() string {
	return l.dbName
}

// RunBenchmark takes in a Benchmark b, a bufio.Reader br, and holders for number of metrics and rows
// and uses those to run the load benchmark
func (l *BenchmarkRunner) RunBenchmark(b Benchmark, workQueues uint) {
	l.br = l.GetBufferedReader()
	cleanupFn := l.useDBCreator(b.GetDBCreator())
	defer cleanupFn()

	channels := l.createChannels(workQueues)

	var wg sync.WaitGroup
	for i := 0; i < int(l.workers); i++ {
		wg.Add(1)
		go l.work(b, &wg, channels[i%len(channels)], i)
	}

	start := time.Now()
	l.scan(b, channels)

	for _, c := range channels {
		c.close()
	}
	wg.Wait()
	end := time.Now()

	l.summary(end.Sub(start))
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

// useDBCreator handles a DBCreator by running it according to flags set by the
// user. The function returns a function that the caller should defer or run
// when the benchmark is finished
func (l *BenchmarkRunner) useDBCreator(dbc DBCreator) func() {
	// empty function to 'defer' from caller
	fn := func() {}

	if l.doLoad {
		// DBCreator should still be Init'd even if -do-create-db is false since
		// it can initialize the connecting session
		dbc.Init()
		switch dbcc := dbc.(type) {
		case DBCreatorCloser:
			fn = dbcc.Close
		}

		exists := dbc.DBExists(l.dbName)
		if exists && l.doAbortOnExist {
			panic(fmt.Sprintf(errDBExistsFmt, l.dbName))
		}
		if l.doCreateDB {
			if exists {
				err := dbc.RemoveOldDB(l.dbName)
				if err != nil {
					panic(err)
				}
			}
			err := dbc.CreateDB(l.dbName)
			if err != nil {
				panic(err)
			}
		}
		switch dbcp := dbc.(type) {
		case DBCreatorPost:
			dbcp.PostCreateDB(l.dbName)
		}
	}
	return fn
}

func (l *BenchmarkRunner) createChannels(workQueues uint) []*duplexChannel {
	channels := []*duplexChannel{}
	maxPartitions := workQueues
	if workQueues == WorkerPerQueue {
		maxPartitions = l.workers
	} else if workQueues > l.workers {
		panic(fmt.Sprintf("cannot have more work queues (%d) than workers (%d)", workQueues, l.workers))
	}
	perQueue := int(math.Ceil(float64(l.workers) / float64(maxPartitions)))
	for i := uint(0); i < maxPartitions; i++ {
		channels = append(channels, newDuplexChannel(perQueue))
	}

	return channels
}

// scan launches any needed reporting mechanism and proceeds to scan input data
// to distribute to workers
func (l *BenchmarkRunner) scan(b Benchmark, channels []*duplexChannel) uint64 {
	if l.reportingPeriod.Nanoseconds() > 0 {
		go l.report(l.reportingPeriod)
	}
	return scanWithIndexer(channels, l.batchSize, l.limit, l.br, b.GetPointDecoder(l.br), b.GetBatchFactory(), b.GetPointIndexer(uint(len(channels))))
}

// work is the processing function for each worker in the loader
func (l *BenchmarkRunner) work(b Benchmark, wg *sync.WaitGroup, c *duplexChannel, workerNum int) {
	proc := b.GetProcessor()
	proc.Init(workerNum, l.doLoad)
	for b := range c.toWorker {
		metricCnt, rowCnt := proc.ProcessBatch(b, l.doLoad)
		atomic.AddUint64(&l.metricCnt, metricCnt)
		atomic.AddUint64(&l.rowCnt, rowCnt)
		c.sendToScanner()
	}
	switch c := proc.(type) {
	case ProcessorCloser:
		c.Close(l.doLoad)
	}
	wg.Done()
}

// summary prints the summary of statistics from loading
func (l *BenchmarkRunner) summary(took time.Duration) {
	metricRate := float64(l.metricCnt) / float64(took.Seconds())
	printFn("\nSummary:\n")
	printFn("loaded %d metrics in %0.3fsec with %d workers (mean rate %0.2f metrics/sec)\n", l.metricCnt, took.Seconds(), l.workers, metricRate)
	if l.rowCnt > 0 {
		rowRate := float64(l.rowCnt) / float64(took.Seconds())
		printFn("loaded %d rows in %0.3fsec with %d workers (mean rate %0.2f rows/sec)\n", l.rowCnt, took.Seconds(), l.workers, rowRate)
	}
}

// report handles periodic reporting of loading stats
func (l *BenchmarkRunner) report(period time.Duration) {
	start := time.Now()
	prevTime := start
	prevColCount := uint64(0)
	prevRowCount := uint64(0)

	printFn("time,per. metric/s,metric total,overall metric/s,per. row/s,row total,overall row/s\n")
	for now := range time.NewTicker(period).C {
		cCount := atomic.LoadUint64(&l.metricCnt)
		rCount := atomic.LoadUint64(&l.rowCnt)

		sinceStart := now.Sub(start)
		took := now.Sub(prevTime)
		colrate := float64(cCount-prevColCount) / float64(took.Seconds())
		overallColRate := float64(cCount) / float64(sinceStart.Seconds())
		if rCount > 0 {
			rowrate := float64(rCount-prevRowCount) / float64(took.Seconds())
			overallRowRate := float64(rCount) / float64(sinceStart.Seconds())
			printFn("%d,%0.2f,%E,%0.2f,%0.2f,%E,%0.2f\n", now.Unix(), colrate, float64(cCount), overallColRate, rowrate, float64(rCount), overallRowRate)
		} else {
			printFn("%d,%0.2f,%E,%0.2f,-,-,-\n", now.Unix(), colrate, float64(cCount), overallColRate)
		}

		prevColCount = cCount
		prevRowCount = rCount
		prevTime = now
	}
}
