package query

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime/pprof"
	"sync"
	"time"
)

const (
	LabelAllQueries  = "all queries"
	LabelColdQueries = "cold queries"
	LabelWarmQueries = "warm queries"
)

// BenchmarkRunner contains the common components for running a query benchmarking
// program against a database.
type BenchmarkRunner struct {
	StatProcessor *StatProcessor
	Workers       int

	scanner        *scanner
	limit          uint64
	memProfile     string
	printResponses bool
	debug          int
}

// NewBenchmarkRunner creates a new instance of BenchmarkRunner which is
// common functionality to be used by query benchmarker programs
func NewBenchmarkRunner() *BenchmarkRunner {
	ret := &BenchmarkRunner{}
	sp := &StatProcessor{
		statPool: GetStatPool(),
		Limit:    &ret.limit,
	}
	ret.scanner = newScanner(&ret.limit)
	ret.StatProcessor = sp
	flag.Uint64Var(&sp.BurnIn, "burn-in", 0, "Number of queries to ignore before collecting statistics.")
	flag.Uint64Var(&ret.limit, "limit", 0, "Limit the number of queries to send, 0 = no limit")
	flag.Uint64Var(&sp.printInterval, "print-interval", 100, "Print timing stats to stderr after this many queries (0 to disable)")
	flag.StringVar(&ret.memProfile, "memprofile", "", "Write a memory profile to this file.")
	flag.IntVar(&ret.Workers, "workers", 1, "Number of concurrent requests to make.")
	flag.BoolVar(&sp.PrewarmQueries, "prewarm-queries", false, "Run each query twice in a row so the warm query is guaranteed to be a cache hit")
	flag.BoolVar(&ret.printResponses, "print-responses", false, "Pretty print response bodies for correctness checking (default false).")
	flag.IntVar(&ret.debug, "debug", 0, "Whether to print debug messages.")

	return ret
}

// ResetLimit changes the number of queries to run, with 0 being all of them
func (b *BenchmarkRunner) ResetLimit(limit uint64) {
	b.limit = limit
}

// DoPrintResponses indicates whether responses for queries should be printed
func (b *BenchmarkRunner) DoPrintResponses() bool {
	return b.printResponses
}

// DebugLevel returns the level of debug messages for this benchmark
func (b *BenchmarkRunner) DebugLevel() int {
	return b.debug
}

// ProcessQueryFunc is a function that is used by a gorountine to process a Query
type ProcessQueryFunc func(*sync.WaitGroup, int)

// Run does the bulk of the benchmark execution. It launches a gorountine to track
// stats, creates workers to process queries, read in the input, execute the queries,
// and then does cleanup.
func (b *BenchmarkRunner) Run(queryPool *sync.Pool, queryChan chan Query, queryFn ProcessQueryFunc) {
	// Launch the stats processor:
	go b.StatProcessor.Process(b.Workers)

	// Launch the query processors:
	var wg sync.WaitGroup
	for i := 0; i < b.Workers; i++ {
		wg.Add(1)
		go queryFn(&wg, i)
	}

	// Read in jobs, closing the job channel when done:
	input := bufio.NewReaderSize(os.Stdin, 1<<20)
	wallStart := time.Now()
	b.scanner.setReader(input).scan(queryPool, queryChan)
	close(queryChan)

	// Block for workers to finish sending requests, closing the stats
	// channel when done:
	wg.Wait()
	b.StatProcessor.CloseAndWait()

	wallEnd := time.Now()
	wallTook := wallEnd.Sub(wallStart)
	_, err := fmt.Printf("wall clock time: %fsec\n", float64(wallTook.Nanoseconds())/1e9)
	if err != nil {
		log.Fatal(err)
	}

	// (Optional) create a memory profile:
	if len(b.memProfile) > 0 {
		f, err := os.Create(b.memProfile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.WriteHeapProfile(f)
		f.Close()
	}
}
