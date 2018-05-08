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

// BenchmarkComponents contains the common components for running a query benchmarking
// program against a database.
type BenchmarkComponents struct {
	Scanner       *QueryScanner
	StatProcessor *StatProcessor
	Workers       int

	limit      uint64
	memProfile string
}

// NewBenchmarkComponents creates a new instance of BenchmarkComponents which is
// common functionality to be used by query benchmarker programs
func NewBenchmarkComponents() *BenchmarkComponents {
	ret := &BenchmarkComponents{}
	sp := &StatProcessor{
		statPool: GetStatPool(),
		Limit:    &ret.limit,
	}
	ret.Scanner = newQueryScanner(&ret.limit)
	ret.StatProcessor = sp
	flag.Uint64Var(&sp.BurnIn, "burn-in", 0, "Number of queries to ignore before collecting statistics.")
	flag.Uint64Var(&ret.limit, "limit", 0, "Limit the number of queries to send, 0 = no limit")
	flag.Uint64Var(&sp.printInterval, "print-interval", 100, "Print timing stats to stderr after this many queries (0 to disable)")
	flag.StringVar(&ret.memProfile, "memprofile", "", "Write a memory profile to this file.")
	flag.IntVar(&ret.Workers, "workers", 1, "Number of concurrent requests to make.")
	flag.BoolVar(&sp.PrewarmQueries, "prewarm-queries", false, "Run each query twice in a row so the warm query is guaranteed to be a cache hit")

	return ret
}

// ResetLimit changes the number of queries to run, with 0 being all of them
func (bc *BenchmarkComponents) ResetLimit(limit uint64) {
	bc.limit = limit
}

// ProcessQueryFunc is a function that is used by a gorountine to process a Query
type ProcessQueryFunc func(*sync.WaitGroup, int)

// Run does the bulk of the benchmark execution. It launches a gorountine to track
// stats, creates workers to process queries, read in the input, execute the queries,
// and then does cleanup.
func (bc *BenchmarkComponents) Run(queryPool *sync.Pool, queryChan chan Query, queryFn ProcessQueryFunc) {
	// Launch the stats processor:
	go bc.StatProcessor.Process(bc.Workers)

	// Launch the query processors:
	var wg sync.WaitGroup
	for i := 0; i < bc.Workers; i++ {
		wg.Add(1)
		go queryFn(&wg, i)
	}

	// Read in jobs, closing the job channel when done:
	input := bufio.NewReaderSize(os.Stdin, 1<<20)
	wallStart := time.Now()
	bc.Scanner.SetReader(input).Scan(queryPool, queryChan)
	close(queryChan)

	// Block for workers to finish sending requests, closing the stats
	// channel when done:
	wg.Wait()
	bc.StatProcessor.CloseAndWait()

	wallEnd := time.Now()
	wallTook := wallEnd.Sub(wallStart)
	_, err := fmt.Printf("wall clock time: %fsec\n", float64(wallTook.Nanoseconds())/1e9)
	if err != nil {
		log.Fatal(err)
	}

	// (Optional) create a memory profile:
	if len(bc.memProfile) > 0 {
		f, err := os.Create(bc.memProfile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.WriteHeapProfile(f)
		f.Close()
	}
}
