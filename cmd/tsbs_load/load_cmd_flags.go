package main

import (
	"fmt"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/load"
	"github.com/timescale/tsbs/pkg/data/source"
	"strings"
	"time"
)

const (
	defaultTimeStart   = "2020-01-01T00:00:00Z"
	defaultTimeEnd     = "2020-01-02T00:00:00Z"
	defaultLogInterval = 10 * time.Second
	defaultScale       = 1
)

func addLoaderRunnerFlags(fs *pflag.FlagSet) {
	fs.String(
		"loader.runner.insert-intervals",
		"",
		"Time to wait between each insert, default '' => all workers insert ASAP. '1,2' = worker 1 waits 1s "+
			"between inserts, worker 2 and others wait 2s",
	)
	fs.Bool(
		"loader.runner.hash-workers",
		false,
		"Whether to consistently hash insert data to the same workers (i.e., the data for a particular host "+
			"always goes to the same worker)")
	fs.Bool(
		"loader.runner.do-abort-on-exist",
		false,
		"Whether to abort if a database with the given name already exists.",
	)
	fs.Duration("loader.runner.reporting-period", 10*time.Second, "Period to report write stats")
	fs.Int64("loader.runner.seed", 0, "PRNG seed (default: 0, which uses the current timestamp)")
	fs.Bool(
		"loader.runner.do-load",
		true,
		"Whether to write data. Set this flag to false to check input read speed.",
	)
	fs.Bool(
		"loader.runner.do-create-db",
		true,
		"Whether to create the database. Disable on all but one client if running on a multi client setup.",
	)
	fs.Uint("loader.runner.workers", 1, "Number of parallel clients inserting")
	fs.Uint64("loader.runner.limit", 0, "Number of items to insert (0 = all of them).")
	fs.String("loader.runner.db-name", "benchmark", "Name of database")
	fs.Uint(
		"loader.runner.batch-size",
		10000,
		"Number of items to batch together in a single insert",
	)
	fs.Bool(
		"loader.runner.flow-control",
		false,
		"Whether to use flow-control when scanning the data and sending to the workers",
	)
	fs.Uint(
		"loader.runner.channel-capacity",
		load.DefaultChannelCapacityFlagVal,
		"(Used only when flow-control=false) Capacity of the channel holding the ready batches.\nIf "+
			"hash-workers=false, one channel is used. If hash-workers=true a channel is created for each worker.\n"+
			"If one channel is full scanning stops until the worker whose channel was full completes a batch.\n"+
			"Default 0 means that:\n\tif hash-workers=false then capacity = 5 * number of workers\n\t"+
			"if hash-workers=true, then capacity = 5 for each worker",
	)
}

func addDataSourceFlags(fs *pflag.FlagSet) {
	fs.String(
		"data-source.type",
		source.SimulatorDataSourceType,
		"Where to load the data from. Valid: "+strings.Join(source.ValidDataSourceTypes, ", "),
	)
	fs.String(
		"data-source.file.location",
		"./file-from-tsbs-generate-data",
		"If data-source.type=FILE, load the data from this file location",
	)
	fs.String("data-source.simulator.use-case", "devops-generic", fmt.Sprintf("Use case to generate."))
	fs.String("data-source.simulator.timestamp-start", defaultTimeStart, "Beginning timestamp (RFC3339).")
	fs.String("data-source.simulator.timestamp-end", defaultTimeEnd, "Ending timestamp (RFC3339).")
	fs.Int("data-source.simulator.debug", 0, "Control level of debug output")
	fs.Int64("data-source.simulator.seed", 0, "PRNG seed (default: 0, which uses the current timestamp)")
	fs.Uint64("data-source.simulator.max-data-points", 0, "Limit the number of data points to generate, 0 = no limit")
	fs.Uint64(
		"data-source.simulator.initial-scale",
		0,
		"Initial scaling variable specific to the use case (e.g., devices in 'devops'). 0 means to use -scale value",
	)
	fs.Uint64(
		"data-source.simulator.max-metric-count",
		100,
		"Max number of metric fields to generate per host. Used only in devops-generic use-case",
	)
	fs.Uint64(
		"data-source.simulator.scale",
		defaultScale,
		"Scaling value specific to use case (e.g., devices in 'devops', trucks in iot).")
	fs.Duration("data-source.simulator.log-interval", defaultLogInterval, "Duration between data points")
}
