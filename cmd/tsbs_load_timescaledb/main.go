// tsbs_load_timescaledb loads a TimescaleDB instance with data from stdin.
//
// If the database exists beforehand, it will be *DROPPED*.
package main

import (
	"fmt"
	"github.com/spf13/pflag"
	"github.com/blagojts/viper"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/load"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/timescaledb"
	"sync"
)

const (
	pgxDriver = "pgx"
	pqDriver  = "postgres"
)

// Parse args:
func initProgramOptions() (*timescaledb.LoadingOptions, *load.BenchmarkRunner, targets.ImplementedTarget) {
	target := timescaledb.NewTarget()
	var config load.BenchmarkRunnerConfig
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
	opts := timescaledb.LoadingOptions{}
	opts.PostgresConnect = viper.GetString("postgres")
	opts.Host = viper.GetString("host")
	opts.DBname = viper.GetString("db-name")
	opts.Port = viper.GetString("port")
	opts.User = viper.GetString("user")
	opts.Pass = viper.GetString("pass")
	opts.ConnDB = viper.GetString("admin-db-name")
	opts.LogBatches = viper.GetBool("log-batches")

	opts.UseHypertable = viper.GetBool("use-hypertable")
	opts.UseJSON = viper.GetBool("use-jsonb-tags")
	opts.InTableTag = viper.GetBool("in-table-partition-tag")

	opts.NumberPartitions = viper.GetInt("partitions")
	opts.ChunkTime = viper.GetDuration("chunk-time")

	opts.TimeIndex = viper.GetBool("time-index")
	opts.TimePartitionIndex = viper.GetBool("time-partition-index")
	opts.PartitionIndex = viper.GetBool("partition-index")
	opts.FieldIndex = viper.GetString("field-index")
	opts.FieldIndexCount = viper.GetInt("field-index-count")

	opts.ProfileFile = viper.GetString("write-profile")
	opts.ReplicationStatsFile = viper.GetString("write-replication-stats")
	opts.CreateMetricsTable = viper.GetBool("create-metrics-table")

	opts.ForceTextFormat = viper.GetBool("force-text-format")
	if opts.ForceTextFormat {
		opts.Driver = pqDriver
	} else {
		opts.Driver = pgxDriver
	}
	loader := load.GetBenchmarkRunner(config)
	return &opts, loader, target
}

func main() {
	opts, loader, target := initProgramOptions()

	// If specified, generate a performance profile
	if len(opts.ProfileFile) > 0 {
		go profileCPUAndMem(opts.ProfileFile)
	}

	var replicationStatsWaitGroup sync.WaitGroup
	if len(opts.ReplicationStatsFile) > 0 {
		go OutputReplicationStats(opts.GetConnectString(), opts.ReplicationStatsFile, &replicationStatsWaitGroup)
	}

	benchmark, err := target.Benchmark(
		&source.DataSourceConfig{
			Type: source.FileDataSourceType,
			File: &source.FileDataSourceConfig{
				Location: loader.FileName,
			},
		}, nil)
	if err != nil {
		panic(err)
	}
	loader.RunBenchmark(benchmark)

	if len(opts.ReplicationStatsFile) > 0 {
		replicationStatsWaitGroup.Wait()
	}
}
