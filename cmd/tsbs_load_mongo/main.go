// bulk_load_mongo loads a Mongo daemon with data from stdin.
//
// Any existing collections in the database will be removed.
package main

import (
	"flag"
	"time"

	"github.com/timescale/tsbs/load"
)

const (
	collectionName     = "point_data"
	aggDocID           = "doc_id"
	aggDateFmt         = "20060102_15" // see Go docs for how we arrive at this time format
	aggKeyID           = "key_id"
	aggInsertBatchSize = 500 // found via trial-and-error
	timestampField     = "timestamp_ns"
)

// Program option vars:
var (
	daemonURL    string
	documentPer  bool
	writeTimeout time.Duration
)

// Global vars
var (
	loader *load.BenchmarkRunner
)

// Parse args:
func init() {
	loader = load.GetBenchmarkRunner()

	flag.StringVar(&daemonURL, "url", "localhost:27017", "Mongo URL.")
	flag.DurationVar(&writeTimeout, "write-timeout", 10*time.Second, "Write timeout.")
	flag.BoolVar(&documentPer, "document-per-event", false, "Whether to use one document per event or aggregate by hour")

	flag.Parse()
}

func main() {
	var benchmark load.Benchmark
	var workQueues uint
	if documentPer {
		benchmark = newNaiveBenchmark(loader)
		workQueues = load.SingleQueue
	} else {
		benchmark = newAggBenchmark(loader)
		workQueues = load.WorkerPerQueue
	}

	loader.RunBenchmark(benchmark, workQueues)
}
