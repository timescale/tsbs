// tsbs_load_timescaledb loads a TimescaleDB instance with data from stdin.
//
// If the database exists beforehand, it will be *DROPPED*.
package main

import (
	"fmt"
	"sync"

	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/load"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets/timescaledb"
)

// Parse args:
func initProgramOptions() (*timescaledb.LoadingOptions, load.BenchmarkRunner, *load.BenchmarkRunnerConfig) {
	target := timescaledb.NewTarget()
	loaderConf := load.BenchmarkRunnerConfig{}
	loaderConf.AddToFlagSet(pflag.CommandLine)
	target.TargetSpecificFlags("", pflag.CommandLine)
	pflag.Parse()

	err := utils.SetupConfigFile()

	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}

	if err := viper.Unmarshal(&loaderConf); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}
	opts := timescaledb.LoadingOptions{}
	viper.SetTypeByDefaultValue(true)
	opts.PostgresConnect = viper.GetString("postgres")
	opts.Host = viper.GetString("host")
	opts.Port = viper.GetString("port")
	opts.User = viper.GetString("user")
	opts.Pass = viper.GetString("pass")
	opts.ConnDB = viper.GetString("admin-db-name")
	opts.LogBatches = viper.GetBool("log-batches")

	opts.UseHypertable = viper.GetBool("use-hypertable")
	opts.ChunkTime = viper.GetDuration("chunk-time")

	opts.UseJSON = viper.GetBool("use-jsonb-tags")

	// This must be set to 'true' if you are going to test
	// distributed hypertable queries and insert. Replication
	// factor must also be set to true for distributed hypertables
	opts.InTableTag = viper.GetBool("in-table-partition-tag")

	// 	We currently use `create_hypertable` for all variations. When
	//   `replication-factor`>=1, we automatically create a distributed
	//   hypertable.
	opts.ReplicationFactor = viper.GetInt("replication-factor")
	// Currently ignored for distributed hypertables. We assume all
	// data nodes will be used based on the partition-column above
	opts.NumberPartitions = viper.GetInt("partitions")

	opts.TimeIndex = viper.GetBool("time-index")
	opts.TimePartitionIndex = viper.GetBool("time-partition-index")
	opts.PartitionIndex = viper.GetBool("partition-index")
	opts.FieldIndex = viper.GetString("field-index")
	opts.FieldIndexCount = viper.GetInt("field-index-count")

	opts.ProfileFile = viper.GetString("write-profile")
	opts.ReplicationStatsFile = viper.GetString("write-replication-stats")
	opts.CreateMetricsTable = viper.GetBool("create-metrics-table")

	opts.ForceTextFormat = viper.GetBool("force-text-format")
	opts.UseInsert = viper.GetBool("use-insert")

	loader := load.GetBenchmarkRunner(loaderConf)
	return &opts, loader, &loaderConf
}

func main() {
	opts, loader, loaderConf := initProgramOptions()

	// If specified, generate a performance profile
	if len(opts.ProfileFile) > 0 {
		go profileCPUAndMem(opts.ProfileFile)
	}

	var replicationStatsWaitGroup sync.WaitGroup
	if len(opts.ReplicationStatsFile) > 0 {
		go OutputReplicationStats(
			opts.GetConnectString(loader.DatabaseName()), opts.ReplicationStatsFile, &replicationStatsWaitGroup,
		)
	}

	benchmark, err := timescaledb.NewBenchmark(loaderConf.DBName, opts, &source.DataSourceConfig{
		Type: source.FileDataSourceType,
		File: &source.FileDataSourceConfig{Location: loaderConf.FileName},
	})
	if err != nil {
		panic(err)
	}
	loader.RunBenchmark(benchmark)

	if len(opts.ReplicationStatsFile) > 0 {
		replicationStatsWaitGroup.Wait()
	}
}
