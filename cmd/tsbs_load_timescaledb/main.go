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
	opts.InTableTag = viper.GetBool("in-table-partition-tag")

	/* These two settings determine if we are creating a distributed
	/ hypertable or not, and if so, what the replication factor should be

	    | partitions | replication-factor | hypertable type created|
	    |------------|--------------------|------------------------|
	    | null, 0, 1 |  null or any int   | regular hypertable     |
	    | >1         |  null              | distributed hypertable, rf=1 |
	    | >1         |  any int           | distributed hypertable, rf = setting |

	*/

	opts.NumberPartitions = viper.GetInt("partitions")
	opts.ReplicationFactor = viper.GetInt("replication-factor")

	opts.TimeIndex = viper.GetBool("time-index")
	opts.TimePartitionIndex = viper.GetBool("time-partition-index")
	opts.PartitionIndex = viper.GetBool("partition-index")
	opts.FieldIndex = viper.GetString("field-index")
	opts.FieldIndexCount = viper.GetInt("field-index-count")

	opts.ProfileFile = viper.GetString("write-profile")
	opts.ReplicationStatsFile = viper.GetString("write-replication-stats")
	opts.CreateMetricsTable = viper.GetBool("create-metrics-table")

	opts.ForceTextFormat = viper.GetBool("force-text-format")
	opts.UseCopy = viper.GetBool("use-copy")

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
