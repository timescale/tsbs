package load

import (
	"encoding/json"
	"fmt"
	"github.com/timescale/tsbs/pkg/targets"
	"io/ioutil"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/load/insertstrategy"
)

const (
	// defaultBatchSize - default size of batches to be inserted
	defaultBatchSize                = 10000
	DefaultChannelCapacityFlagVal   = 0
	defaultChannelCapacityPerWorker = 5
	errDBExistsFmt                  = "database \"%s\" exists: aborting."
)

// change for more useful testing
var (
	printFn = fmt.Printf
	fatal   = log.Fatalf
)

// BenchmarkRunnerConfig contains all the configuration information required for running BenchmarkRunner.
type BenchmarkRunnerConfig struct {
	DBName              string        `yaml:"db-name" mapstructure:"db-name" json:"db-name"`
	BatchSize           uint          `yaml:"batch-size" mapstructure:"batch-size" json:"batch-size"`
	Workers             uint          `yaml:"workers" mapstructure:"workers" json:"workers"`
	Limit               uint64        `yaml:"limit" mapstructure:"limit" json:"limit"`
	DoLoad              bool          `yaml:"do-load" mapstructure:"do-load" json:"do-load"`
	DoCreateDB          bool          `yaml:"do-create-db" mapstructure:"do-create-db" json:"do-create-db"`
	DoAbortOnExist      bool          `yaml:"do-abort-on-exist" mapstructure:"do-abort-on-exist" json:"do-abort-on-exist"`
	ReportingPeriod     time.Duration `yaml:"reporting-period" mapstructure:"reporting-period" json:"reporting-period"`
	HashWorkers         bool          `yaml:"hash-workers" mapstructure:"hash-workers" json:"hash-workers"`
	NoFlowControl       bool          `yaml:"no-flow-control" mapstructure:"no-flow-control" json:"no-flow-control"`
	ChannelCapacity     uint          `yaml:"channel-capacity" mapstructure:"channel-capacity" json:"channel-capacity"`
	InsertIntervals     string        `yaml:"insert-intervals" mapstructure:"insert-intervals" json:"insert-intervals"`
	InsertIntervalsUnit string        `yaml:"insert-intervals-unit" mapstructure:"insert-intervals-unit" json:"insert-intervals-unit"`
	ResultsFile         string        `yaml:"results-file" mapstructure:"results-file" json:"results-file"`
	// deprecated, should not be used in other places other than tsbs_load_xx commands
	FileName string `yaml:"file" mapstructure:"file" json:"file"`
	Seed     int64  `yaml:"seed" mapstructure:"seed" json:"seed"`
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
	fs.String("insert-intervals", "", "Time to wait between each insert, default '' => all workers insert ASAP. '1,2' = worker 1 waits 1s between inserts, worker 2 and others wait 2s. (Unit adjustable with insert-intervals-unit)")
	fs.String("insert-intervals-unit", "second", "Unit for insert intervals. Options: second, millisecond, microsecond.")
	fs.Bool("hash-workers", false, "Whether to consistently hash insert data to the same workers (i.e., the data for a particular host always goes to the same worker)")
	fs.String("results-file", "", "Write the test results summary json to this file")
}

type BenchmarkRunner interface {
	DatabaseName() string
	RunBenchmark(b targets.Benchmark)
}

// CommonBenchmarkRunner is responsible for initializing and storing common
// flags across all database systems and ultimately running a supplied Benchmark
type CommonBenchmarkRunner struct {
	BenchmarkRunnerConfig
	metricCnt      uint64
	rowCnt         uint64
	initialRand    *rand.Rand
	sleepRegulator insertstrategy.SleepRegulator
}

// GetBenchmarkRunnerWithBatchSize returns the singleton CommonBenchmarkRunner for use in a benchmark program
// with specified batch size.
func GetBenchmarkRunner(c BenchmarkRunnerConfig) BenchmarkRunner {
	loader := CommonBenchmarkRunner{}
	loader.BenchmarkRunnerConfig = c
	// If the configuration batch size is 0 use the default batch size.
	if loader.BatchSize == 0 {
		loader.BatchSize = defaultBatchSize
	}

	loader.initialRand = rand.New(rand.NewSource(loader.Seed))

	var err error
	if c.InsertIntervals == "" {
		loader.sleepRegulator = insertstrategy.NoWait()
	} else {
		loader.sleepRegulator, err = insertstrategy.NewSleepRegulator(c.InsertIntervals, c.InsertIntervalsUnit, int(loader.Workers), loader.initialRand)
		if err != nil {
			panic(fmt.Sprintf("could not initialize BenchmarkRunner: %v", err))
		}
	}
	if !c.NoFlowControl {
		return &loader
	}

	if c.ChannelCapacity == DefaultChannelCapacityFlagVal {
		if c.HashWorkers {
			loader.ChannelCapacity = defaultChannelCapacityPerWorker
		} else {
			loader.ChannelCapacity = c.Workers * defaultChannelCapacityPerWorker
		}
	}

	return &noFlowBenchmarkRunner{loader}
}

// DatabaseName returns the value of the --db-name flag (name of the database to store data)
func (l *CommonBenchmarkRunner) DatabaseName() string {
	return l.DBName
}

func (l *CommonBenchmarkRunner) preRun(b targets.Benchmark) (*sync.WaitGroup, *time.Time) {
	// Create required DB
	if b.GetDBCreator() != nil {
		cleanupFn := l.useDBCreator(b.GetDBCreator())
		defer cleanupFn()
	}

	if l.ReportingPeriod.Nanoseconds() > 0 {
		go l.report(l.ReportingPeriod)
	}
	wg := &sync.WaitGroup{}
	wg.Add(int(l.Workers))
	start := time.Now()
	return wg, &start
}

func (l *CommonBenchmarkRunner) postRun(wg *sync.WaitGroup, start *time.Time) {
	// Wait for all workers to finish
	wg.Wait()
	end := time.Now()
	took := end.Sub(*start)
	l.summary(took)
	if l.BenchmarkRunnerConfig.ResultsFile != "" {
		metricRate := float64(l.metricCnt) / took.Seconds()
		rowRate := float64(l.rowCnt) / took.Seconds()
		l.saveTestResult(took, *start, end, metricRate, rowRate)
	}
}

func (l *CommonBenchmarkRunner) saveTestResult(took time.Duration, start time.Time, end time.Time, metricRate, rowRate float64) {
	totals := make(map[string]interface{})
	totals["metricRate"] = metricRate
	if l.rowCnt > 0 {
		totals["rowRate"] = rowRate
	}

	testResult := LoaderTestResult{
		ResultFormatVersion: LoaderTestResultVersion,
		RunnerConfig:        l.BenchmarkRunnerConfig,
		StartTime:           start.Unix(),
		EndTime:             end.Unix(),
		DurationMillis:      took.Milliseconds(),
		Totals:              totals,
	}

	_, _ = fmt.Printf("Saving results json file to %s\n", l.BenchmarkRunnerConfig.ResultsFile)
	file, err := json.MarshalIndent(testResult, "", " ")
	if err != nil {
		log.Fatal(err)
	}

	err = ioutil.WriteFile(l.BenchmarkRunnerConfig.ResultsFile, file, 0644)
	if err != nil {
		log.Fatal(err)
	}
}

// RunBenchmark takes in a Benchmark b and uses it to run the load benchmark
func (l *CommonBenchmarkRunner) RunBenchmark(b targets.Benchmark) {
	wg, start := l.preRun(b)
	var numChannels, capacity uint
	if l.HashWorkers {
		numChannels = l.Workers
		capacity = 1
	} else {
		numChannels = 1
		capacity = l.Workers
	}

	channels := l.createChannels(numChannels, capacity)

	// Launch all worker processes in background
	for i := uint(0); i < l.Workers; i++ {
		go l.work(b, wg, channels[i%numChannels], i)
	}

	// Start scan process - actual data read process
	scanWithFlowControl(channels, l.BatchSize, l.Limit, b.GetDataSource(), b.GetBatchFactory(), b.GetPointIndexer(uint(len(channels))))
	// After scan process completed (no more data to come) - begin shutdown process

	// Close all communication channels to/from workers
	for _, c := range channels {
		c.close()
	}

	l.postRun(wg, start)
}

// useDBCreator handles a DBCreator by running it according to flags set by the
// user. The function returns a function that the caller should defer or run
// when the benchmark is finished
func (l *CommonBenchmarkRunner) useDBCreator(dbc targets.DBCreator) func() {
	// Empty function to 'defer' from caller
	closeFn := func() {}

	// DBCreator should still be Init'd even if -do-create-db is false since
	// it can initialize the connecting session
	dbc.Init()
	if l.DoLoad {
		switch dbcc := dbc.(type) {
		case targets.DBCreatorCloser:
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
		case targets.DBCreatorPost:
			err := dbcp.PostCreateDB(l.DBName)
			if err != nil {
				log.Println("could not execute PostCreateDB:" + err.Error())
				panic(err)
			}
		}
	}
	return closeFn
}

// createChannels create channels from which workers would receive tasks
func (l *CommonBenchmarkRunner) createChannels(numChannels, capacity uint) []*duplexChannel {
	// Result - channels to be created
	var channels []*duplexChannel
	// Create duplex communication channels
	for i := uint(0); i < numChannels; i++ {
		channels = append(channels, newDuplexChannel(int(capacity)))
	}

	return channels
}

// work is the processing function for each worker in the loader
func (l *CommonBenchmarkRunner) work(b targets.Benchmark, wg *sync.WaitGroup, c *duplexChannel, workerNum uint) {

	// Prepare processor
	proc := b.GetProcessor()
	proc.Init(int(workerNum), l.DoLoad, l.HashWorkers)

	// Process batches coming from duplexChannel.toWorker queue
	// and send ACKs into duplexChannel.toScanner queue
	for batch := range c.toWorker {
		startedWorkAt := time.Now()
		metricCnt, rowCnt := proc.ProcessBatch(batch, l.DoLoad)
		atomic.AddUint64(&l.metricCnt, metricCnt)
		atomic.AddUint64(&l.rowCnt, rowCnt)
		c.sendToScanner()
		l.timeToSleep(workerNum, startedWorkAt)
	}

	// Close proc if necessary
	switch c := proc.(type) {
	case targets.ProcessorCloser:
		c.Close(l.DoLoad)
	}

	wg.Done()
}

func (l *CommonBenchmarkRunner) timeToSleep(workerNum uint, startedWorkAt time.Time) {
	if l.sleepRegulator != nil {
		l.sleepRegulator.Sleep(int(workerNum), startedWorkAt)
	}
}

// summary prints the summary of statistics from loading
func (l *CommonBenchmarkRunner) summary(took time.Duration) {
	metricRate := float64(l.metricCnt) / took.Seconds()
	printFn("\nSummary:\n")
	printFn("loaded %d metrics in %0.3fsec with %d workers (mean rate %0.2f metrics/sec)\n", l.metricCnt, took.Seconds(), l.Workers, metricRate)
	if l.rowCnt > 0 {
		rowRate := float64(l.rowCnt) / float64(took.Seconds())
		printFn("loaded %d rows in %0.3fsec with %d workers (mean rate %0.2f rows/sec)\n", l.rowCnt, took.Seconds(), l.Workers, rowRate)
	}
}

// report handles periodic reporting of loading stats
func (l *CommonBenchmarkRunner) report(period time.Duration) {
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
