// bulk_load_mongo loads a Mongo daemon with data from stdin.
//
// Any existing collections in the database will be removed.
package main

import (
	"fmt"
	"log"
	"time"

	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/load"
	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/constants"
	"github.com/timescale/tsbs/pkg/targets/initializers"
)

const (
	collectionName     = "point_data"
	aggDocID           = "doc_id"
	aggDateFmt         = "20060102_15" // see Go docs for how we arrive at this time format
	aggKeyID           = "key_id"
	aggInsertBatchSize = 500 // found via trial-and-error
	timestampField     = "time"
)

// Program option vars:
var (
	daemonURL            string
	documentPer          bool
	writeTimeout         time.Duration
	timeseriesCollection bool
	retryableWrites      bool
	orderedInserts       bool
	randomFieldOrder     bool
	collectionSharded    bool
	numInitChunks        uint
	shardKeySpec         string
	balancerOn       bool
)

// Global vars
var (
	loader load.BenchmarkRunner
	config load.BenchmarkRunnerConfig
	target targets.ImplementedTarget
)

// Parse args:
func init() {
	target = initializers.GetTarget(constants.FormatMongo)
	config = load.BenchmarkRunnerConfig{}
	config.AddToFlagSet(pflag.CommandLine)
	target.TargetSpecificFlags("", pflag.CommandLine)

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
	timeseriesCollection = viper.GetBool("timeseries-collection")
	retryableWrites = viper.GetBool("retryable-writes")
	orderedInserts = viper.GetBool("ordered-inserts")
	randomFieldOrder = viper.GetBool("random-field-order")
	collectionSharded = viper.GetBool("collection-sharded")
	numInitChunks = viper.GetUint("number-initial-chunks")
	shardKeySpec = viper.GetString("shard-key-spec")
	balancerOn = viper.GetBool("balancer-on")
	if documentPer {
		config.HashWorkers = false
	} else {
		config.HashWorkers = true
	}
	
	if !documentPer && timeseriesCollection {
		log.Fatal("Must set document-per-event=true in order to use timeseries-collection=true")
	}
	loader = load.GetBenchmarkRunner(config)
}

func main() {
	var benchmark targets.Benchmark
	if documentPer {
		benchmark = newNaiveBenchmark(loader, &config)
	} else {
		benchmark = newAggBenchmark(loader, &config)
	}

	loader.RunBenchmark(benchmark)
}
