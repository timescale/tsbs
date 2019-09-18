package load

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/load/insertstrategy"
)

const (
	// defaultBatchSize - default size of batches to be inserted
	defaultBatchSize = 10000
	defaultReadSize  = 4 << 20 // 4 MB

	// WorkerPerQueue is the value for assigning each worker its own queue of batches
	WorkerPerQueue = 0
	// SingleQueue is the value for using a single shared queue across all workers
	SingleQueue = 1

	errDBExistsFmt = "database \"%s\" exists: aborting."
)

// change for more useful testing
var (
	printFn = fmt.Printf
	fatal   = log.Fatalf
)

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

	// GetDBCreator returns the DBCreator to use for this Benchmark
	GetDBCreator() DBCreator
}

// BenchmarkRunnerConfig contains all the configuration information required for running BenchmarkRunner.
type BenchmarkRunnerConfig struct {
	DBName          string        `mapstructure:"db-name"`
	BatchSize       uint          `mapstructure:"batch-size"`
	Workers         uint          `mapstructure:"workers"`
	Limit           uint64        `mapstructure:"limit"`
	DoLoad          bool          `mapstructure:"do-load"`
	DoCreateDB      bool          `mapstructure:"do-create-db"`
	DoAbortOnExist  bool          `mapstructure:"do-abort-on-exist"`
	ReportingPeriod time.Duration `mapstructure:"reporting-period"`
	FileName        string        `mapstructure:"file"`
	Seed            int64         `mapstructure:"seed"`
}

// AddToFlagSet adds command line flags needed by the BenchmarkRunnerConfig to the flag set.
func (c BenchmarkRunnerConfig) AddToFlagSet(fs *pflag.FlagSet) {
	fs.String("db-name", "benchmark", "Name of database")
	fs.Uint("batch-size", defaultBatchSize, "Number of items to batch together in a single insert")
	fs.Uint("workers", 1, "Number of parallel clients inserting")
	fs.Uint64("limit", 0, "Number of items to insert (0 = all of them).")
	fs.Bool("do-load", true, "Whether to write data. Set this flag to false to check input read speed.")
	fs.Bool("do-create-db", true, "Whether to create the database. Disable on all but one client if running on a multi client setup.")
	fs.Bool("do-abort-on-exist", false, "Whether to abort if a database with the given name already exists.")
	fs.Duration("reporting-period", 10*time.Second, "Period to report write stats")
	fs.String("file", "", "File name to read data from")
	fs.Int64("seed", 0, "PRNG seed (default: 0, which uses the current timestamp)")
}

// BenchmarkRunner is responsible for initializing and storing common
// flags across all database systems and ultimately running a supplied Benchmark
type BenchmarkRunner struct {
	BenchmarkRunnerConfig
	br             *bufio.Reader
	metricCnt      uint64
	rowCnt         uint64
	initialRand    *rand.Rand
	sleepRegulator insertstrategy.SleepRegulator
}

var loader = &BenchmarkRunner{}

// GetBenchmarkRunner returns the singleton BenchmarkRunner for use in a benchmark program
// with a default batch size
func GetBenchmarkRunner(c BenchmarkRunnerConfig) *BenchmarkRunner {
	return GetBenchmarkRunnerWithBatchSize(c, defaultBatchSize)
}

// GetBenchmarkRunnerWithBatchSize returns the singleton BenchmarkRunner for use in a benchmark program
// with specified batch size.
func GetBenchmarkRunnerWithBatchSize(c BenchmarkRunnerConfig, batchSize uint) *BenchmarkRunner {
	loader.BenchmarkRunnerConfig = c

	// If the configuration batch size is at default, we use the supplied batch size instead.
	if c.BatchSize == defaultBatchSize {
		c.BatchSize = batchSize
	}

	loader.initialRand = rand.New(rand.NewSource(loader.Seed))

	var insertIntervals string
	flag.StringVar(&insertIntervals, "insert-intervals", "", "Time to wait between each insert, default '' => all workers insert ASAP. '1,2' = worker 1 waits 1s between inserts, worker 2 and others wait 2s")
	var err error
	if insertIntervals == "" {
		loader.sleepRegulator = insertstrategy.NoWait()
	} else {
		loader.sleepRegulator, err = insertstrategy.NewSleepRegulator(insertIntervals, int(loader.Workers), loader.initialRand)
		if err != nil {
			panic(fmt.Sprintf("could not initialize BenchmarkRunner: %v", err))
		}
	}

	return loader
}

// DatabaseName returns the value of the --db-name flag (name of the database to store data)
func (l *BenchmarkRunner) DatabaseName() string {
	return l.DBName
}

// RunBenchmark takes in a Benchmark b, a bufio.Reader br, and holders for number of metrics and rows
// and uses those to run the load benchmark
func (l *BenchmarkRunner) RunBenchmark(b Benchmark, workQueues uint) {
	l.br = l.GetBufferedReader()

	// Create required DB
	cleanupFn := l.useDBCreator(b.GetDBCreator())
	defer cleanupFn()

	channels := l.createChannels(workQueues)

	// Launch all worker processes in background
	var wg sync.WaitGroup
	numChannels := len(channels)
	for i := 0; i < int(l.Workers); i++ {
		wg.Add(1)

		go l.work(b, &wg, channels[i%numChannels], i)
	}

	// Start scan process - actual data read process
	start := time.Now()
	l.scan(b, channels)

	// After scan process completed (no more data to come) - begin shutdown process

	// Close all communication channels to/from workers
	for _, c := range channels {
		c.close()
	}

	// Wait for all workers to finish
	wg.Wait()
	end := time.Now()

	l.summary(end.Sub(start))
}

// GetBufferedReader returns the buffered Reader that should be used by the loader
func (l *BenchmarkRunner) GetBufferedReader() *bufio.Reader {
	if l.br == nil {
		if len(l.FileName) > 0 {
			// Read from specified file
			file, err := os.Open(l.FileName)
			if err != nil {
				fatal("cannot open file for read %s: %v", l.FileName, err)
				return nil
			}
			l.br = bufio.NewReaderSize(file, defaultReadSize)
		} else {
			// Read from STDIN
			l.br = bufio.NewReaderSize(os.Stdin, defaultReadSize)
		}
	}
	return l.br
}

// useDBCreator handles a DBCreator by running it according to flags set by the
// user. The function returns a function that the caller should defer or run
// when the benchmark is finished
func (l *BenchmarkRunner) useDBCreator(dbc DBCreator) func() {
	// Empty function to 'defer' from caller
	closeFn := func() {}

	if l.DoLoad {
		// DBCreator should still be Init'd even if -do-create-db is false since
		// it can initialize the connecting session
		dbc.Init()

		switch dbcc := dbc.(type) {
		case DBCreatorCloser:
			closeFn = dbcc.Close
		}

		// Check whether required DB already exists
		exists := dbc.DBExists(l.DBName)
		if exists && l.DoAbortOnExist {
			panic(fmt.Sprintf(errDBExistsFmt, l.DBName))
		}

		// Create required DB if need be
		// In case DB already exists - delete it
		if l.DoCreateDB {
			if exists {
				err := dbc.RemoveOldDB(l.DBName)
				if err != nil {
					panic(err)
				}
			}
			err := dbc.CreateDB(l.DBName)
			if err != nil {
				panic(err)
			}
		}

		switch dbcp := dbc.(type) {
		case DBCreatorPost:
			dbcp.PostCreateDB(l.DBName)
		}
	}
	return closeFn
}

// createChannels create channels from which workers would receive tasks
// Number of workers may be different from number of channels, thus we may have
// multiple workers per channel
func (l *BenchmarkRunner) createChannels(workQueues uint) []*duplexChannel {
	// Result - channels to be created
	channels := []*duplexChannel{}

	// How many work queues should be created?
	workQueuesToCreate := int(workQueues)
	if workQueues == WorkerPerQueue {
		workQueuesToCreate = int(l.Workers)
	} else if workQueues > l.Workers {
		panic(fmt.Sprintf("cannot have more work queues (%d) than workers (%d)", workQueues, l.Workers))
	}

	// How many workers would be served by each queue?
	workersPerQueue := int(math.Ceil(float64(l.Workers) / float64(workQueuesToCreate)))

	// Create duplex communication channels
	for i := 0; i < workQueuesToCreate; i++ {
		channels = append(channels, newDuplexChannel(workersPerQueue))
	}

	return channels
}

// scan launches any needed reporting mechanism and proceeds to scan input data
// to distribute to workers
func (l *BenchmarkRunner) scan(b Benchmark, channels []*duplexChannel) uint64 {
	// Start background reporting process
	// TODO why it is here? May be it could be moved one level up?
	if l.ReportingPeriod.Nanoseconds() > 0 {
		go l.report(l.ReportingPeriod)
	}

	// Scan incoming data
	return scanWithIndexer(channels, l.BatchSize, l.Limit, l.br, b.GetPointDecoder(l.br), b.GetBatchFactory(), b.GetPointIndexer(uint(len(channels))))
}

// work is the processing function for each worker in the loader
func (l *BenchmarkRunner) work(b Benchmark, wg *sync.WaitGroup, c *duplexChannel, workerNum int) {

	// Prepare processor
	proc := b.GetProcessor()
	proc.Init(workerNum, l.DoLoad)

	// Process batches coming from duplexChannel.toWorker queue
	// and send ACKs into duplexChannel.toScanner queue
	for b := range c.toWorker {
		startedWorkAt := time.Now()
		metricCnt, rowCnt := proc.ProcessBatch(b, l.DoLoad)
		atomic.AddUint64(&l.metricCnt, metricCnt)
		atomic.AddUint64(&l.rowCnt, rowCnt)
		c.sendToScanner()
		l.timeToSleep(workerNum, startedWorkAt)
	}

	// Close proc if necessary
	switch c := proc.(type) {
	case ProcessorCloser:
		c.Close(l.DoLoad)
	}

	wg.Done()
}

func (l *BenchmarkRunner) timeToSleep(workerNum int, startedWorkAt time.Time) {
	if l.sleepRegulator != nil {
		l.sleepRegulator.Sleep(workerNum, startedWorkAt)
	}
}

// summary prints the summary of statistics from loading
func (l *BenchmarkRunner) summary(took time.Duration) {
	metricRate := float64(l.metricCnt) / float64(took.Seconds())
	printFn("\nSummary:\n")
	printFn("loaded %d metrics in %0.3fsec with %d workers (mean rate %0.2f metrics/sec)\n", l.metricCnt, took.Seconds(), l.Workers, metricRate)
	if l.rowCnt > 0 {
		rowRate := float64(l.rowCnt) / float64(took.Seconds())
		printFn("loaded %d rows in %0.3fsec with %d workers (mean rate %0.2f rows/sec)\n", l.rowCnt, took.Seconds(), l.Workers, rowRate)
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
