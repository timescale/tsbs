// bulk_load_mongo loads a Mongo daemon with data from stdin.
//
// Any existing collections in the database will be removed.
package main

import (
	"fmt"
	"time"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/timescale/tsbs/internal/utils"
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
	var config load.BenchmarkRunnerConfig
	config.AddToFlagSet(pflag.CommandLine)

	pflag.String("url", "localhost:27017", "Mongo URL.")
	pflag.Duration("write-timeout", 10*time.Second, "Write timeout.")
	pflag.Bool("document-per-event", false, "Whether to use one document per event or aggregate by hour")

	pflag.Parse()

	err := utils.SetupConfigFile()

	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}

	if err := viper.Unmarshal(&config); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}

	daemonURL = viper.GetString("url")
	writeTimeout = viper.GetDuration("write-timeout")
	documentPer = viper.GetBool("document-per-event")

	loader = load.GetBenchmarkRunner(config)
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
