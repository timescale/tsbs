package query

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"runtime/pprof"
	"sync"
	"time"

	"github.com/spf13/pflag"
)

const (
	labelAllQueries  = "all queries"
	labelColdQueries = "cold queries"
	labelWarmQueries = "warm queries"

	defaultReadSize = 4 << 20 // 4 MB
)

// BenchmarkRunnerConfig is the configuration of the benchmark runner.
type BenchmarkRunnerConfig struct {
	DBName         string `mapstructure:"db-name"`
	Limit          uint64 `mapstructure:"max-queries"`
	MemProfile     string `mapstructure:"memprofile"`
	Workers        uint   `mapstructure:"workers"`
	PrintResponses bool   `mapstructure:"print-responses"`
	Debug          int    `mapstructure:"debug"`
	FileName       string `mapstructure:"file"`
	BurnIn         uint64 `mapstructure:"burn-in"`
	PrintInterval  uint64 `mapstructure:"print-interval"`
	PrewarmQueries bool   `mapstructure:"prewarm-queries"`
}

// AddToFlagSet adds command line flags needed by the BenchmarkRunnerConfig to the flag set.
func (c BenchmarkRunnerConfig) AddToFlagSet(fs *pflag.FlagSet) {
	fs.String("db-name", "benchmark", "Name of database to use for queries")
	fs.Uint64("burn-in", 0, "Number of queries to ignore before collecting statistics.")
	fs.Uint64("max-queries", 0, "Limit the number of queries to send, 0 = no limit")
	fs.Uint64("print-interval", 100, "Print timing stats to stderr after this many queries (0 to disable)")
	fs.String("memprofile", "", "Write a memory profile to this file.")
	fs.Uint("workers", 1, "Number of concurrent requests to make.")
	fs.Bool("prewarm-queries", false, "Run each query twice in a row so the warm query is guaranteed to be a cache hit")
	fs.Bool("print-responses", false, "Pretty print response bodies for correctness checking (default false).")
	fs.Int("debug", 0, "Whether to print debug messages.")
	fs.String("file", "", "File name to read queries from")
}

// BenchmarkRunner contains the common components for running a query benchmarking
// program against a database.
type BenchmarkRunner struct {
	BenchmarkRunnerConfig
	br      *bufio.Reader
	sp      statProcessor
	scanner *scanner
	ch      chan Query
}

// NewBenchmarkRunner creates a new instance of BenchmarkRunner which is
// common functionality to be used by query benchmarker programs
func NewBenchmarkRunner(config BenchmarkRunnerConfig) *BenchmarkRunner {
	runner := &BenchmarkRunner{BenchmarkRunnerConfig: config}
	runner.scanner = newScanner(&runner.Limit)
	spArgs := &statProcessorArgs{
		limit:          &runner.Limit,
		printInterval:  runner.PrintInterval,
		prewarmQueries: runner.PrewarmQueries,
		burnIn:         runner.BurnIn,
	}

	runner.sp = newStatProcessor(spArgs)
	return runner
}

// SetLimit changes the number of queries to run, with 0 being all of them
func (b *BenchmarkRunner) SetLimit(limit uint64) {
	b.Limit = limit
}

// DoPrintResponses indicates whether responses for queries should be printed
func (b *BenchmarkRunner) DoPrintResponses() bool {
	return b.PrintResponses
}

// DebugLevel returns the level of debug messages for this benchmark
func (b *BenchmarkRunner) DebugLevel() int {
	return b.Debug
}

// DatabaseName returns the name of the database to run queries against
func (b *BenchmarkRunner) DatabaseName() string {
	return b.DBName
}

// ProcessorCreate is a function that creates a new Processor (called in Run)
type ProcessorCreate func() Processor

// Processor is an interface that handles the setup of a query processing worker and executes queries one at a time
type Processor interface {
	// Init initializes at global state for the Processor, possibly based on its worker number / ID
	Init(workerNum int)

	// ProcessQuery handles a given query and reports its stats
	ProcessQuery(q Query, isWarm bool) ([]*Stat, error)
}

// GetBufferedReader returns the buffered Reader that should be used by the loader
func (b *BenchmarkRunner) GetBufferedReader() *bufio.Reader {
	if b.br == nil {
		if len(b.FileName) > 0 {
			// Read from specified file
			file, err := os.Open(b.FileName)
			if err != nil {
				panic(fmt.Sprintf("cannot open file for read %s: %v", b.FileName, err))
			}
			b.br = bufio.NewReaderSize(file, defaultReadSize)
		} else {
			// Read from STDIN
			b.br = bufio.NewReaderSize(os.Stdin, defaultReadSize)
		}
	}
	return b.br
}

// Run does the bulk of the benchmark execution.
// It launches a gorountine to track stats, creates workers to process queries,
// read in the input, execute the queries, and then does cleanup.
func (b *BenchmarkRunner) Run(queryPool *sync.Pool, processorCreateFn ProcessorCreate) {
	if b.Workers == 0 {
		panic("must have at least one worker")
	}

	spArgs := b.sp.getArgs()
	if spArgs.burnIn > b.Limit {
		panic("burn-in is larger than limit")
	}
	b.ch = make(chan Query, b.Workers)

	// Launch the stats processor:
	go b.sp.process(b.Workers)

	// Launch query processors
	var wg sync.WaitGroup
	for i := 0; i < int(b.Workers); i++ {
		wg.Add(1)
		go b.processorHandler(&wg, queryPool, processorCreateFn(), i)
	}

	// Read in jobs, closing the job channel when done:
	// Wall clock start time
	wallStart := time.Now()
	b.scanner.setReader(b.GetBufferedReader()).scan(queryPool, b.ch)
	close(b.ch)

	// Block for workers to finish sending requests, closing the stats channel when done:
	wg.Wait()
	b.sp.CloseAndWait()

	// Wall clock end time
	wallEnd := time.Now()
	wallTook := wallEnd.Sub(wallStart)
	_, err := fmt.Printf("wall clock time: %fsec\n", float64(wallTook.Nanoseconds())/1e9)
	if err != nil {
		log.Fatal(err)
	}

	// (Optional) create a memory profile:
	if len(b.MemProfile) > 0 {
		f, err := os.Create(b.MemProfile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.WriteHeapProfile(f)
		f.Close()
	}
}

func (b *BenchmarkRunner) processorHandler(wg *sync.WaitGroup, queryPool *sync.Pool, processor Processor, workerNum int) {
	processor.Init(workerNum)
	for query := range b.ch {
		stats, err := processor.ProcessQuery(query, false)
		if err != nil {
			panic(err)
		}
		b.sp.send(stats)

		// If PrewarmQueries is set, we run the query as 'cold' first (see above),
		// then we immediately run it a second time and report that as the 'warm' stat.
		// This guarantees that the warm stat will reflect optimal cache performance.
		spArgs := b.sp.getArgs()
		if spArgs.prewarmQueries {
			// Warm run
			stats, err = processor.ProcessQuery(query, true)
			if err != nil {
				panic(err)
			}
			b.sp.sendWarm(stats)
		}
		queryPool.Put(query)
	}
	wg.Done()
}
