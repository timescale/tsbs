// tsbs_load_timescaledb loads a TimescaleDB instance with data from stdin.
//
// If the database exists beforehand, it will be *DROPPED*.
package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/load"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets/timescaledb"
)

const (
	timeValueIdx = "TIME-VALUE"
	valueTimeIdx = "VALUE-TIME"
	pgxDriver    = "pgx"
	pqDriver     = "postgres"
)

// Program option vars:
var (
	postgresConnect string
	host            string
	user            string
	pass            string
	port            string
	connDB          string
	driver          string // postgres or pgx

	useHypertable bool
	logBatches    bool
	useJSON       bool
	inTableTag    bool
	hashWorkers   bool

	numberPartitions int
	chunkTime        time.Duration

	timeIndex           bool
	timePartitionIndex  bool
	partitionIndex      bool
	partitionOnHostname bool
	fieldIndex          string
	fieldIndexCount     int

	profileFile          string
	replicationStatsFile string

	createMetricsTable bool
	forceTextFormat    bool
	tagColumnTypes     []string
	useCopy            bool
	replicationFactor  int
)

type insertData struct {
	tags   string
	fields string
}

// Global vars
var (
	loader     load.BenchmarkRunner
	loaderConf load.BenchmarkRunnerConfig
	opts       timescaledb.LoadingOptions
)

// allows for testing
var fatal = log.Fatalf

// Parse args:
func initProgramOptions() (*timescaledb.LoadingOptions, load.BenchmarkRunner, *load.BenchmarkRunnerConfig) {
	//var config load.BenchmarkRunnerConfig
	loaderConf.AddToFlagSet(pflag.CommandLine)

	pflag.String("postgres", "sslmode=disable", "PostgreSQL connection string")
	pflag.String("host", "localhost", "Hostname of TimescaleDB (PostgreSQL) instance")
	pflag.String("port", "5432", "Which port to connect to on the database host")
	pflag.String("user", "postgres", "User to connect to PostgreSQL as")
	pflag.String("pass", "", "Password for user connecting to PostgreSQL (leave blank if not password protected)")
	pflag.String("admin-db-name", user, "Database to connect to in order to create additional benchmark databases.\n"+
		"By default this is the same as the `user` (i.e., `postgres` if neither is set),\n"+
		"but sometimes a user does not have its own database.")

	pflag.Bool("log-batches", false, "Whether to time individual batches.")

	pflag.Bool("use-hypertable", true, "Whether to make the table a hypertable. Set this flag to false to check input write speed against regular PostgreSQL.")
	pflag.Bool("use-jsonb-tags", false, "Whether tags should be stored as JSONB (instead of a separate table with schema)")
	pflag.Bool("in-table-partition-tag", false, "Whether the partition key (e.g. hostname) should also be in the metrics hypertable")
	// TODO - This flag could potentially be done as a string/enum with other options besides no-hash, round-robin, etc
	pflag.Bool("hash-workers", false, "Whether to consistently hash insert data to the same workers (i.e., the data for a particular host always goes to the same worker)")

	pflag.Int("partitions", 1, "Number of partitions")
	pflag.Duration("chunk-time", 12*time.Hour, "Duration that each chunk should represent, e.g., 12h")

	pflag.BoolVar(&timeIndex, "time-index", true, "Whether to build an index on the time dimension")
	pflag.BoolVar(&timePartitionIndex, "time-partition-index", false, "Whether to build an index on the time dimension, compounded with partition")
	pflag.BoolVar(&partitionOnHostname, "partition-on-hostname", false, "Whether to create the space partition on the hostname column (instead of tags-id)")
	pflag.BoolVar(&partitionIndex, "partition-index", true, "Whether to build an index on the partition key")
	pflag.StringVar(&fieldIndex, "field-index", valueTimeIdx, "index types for tags (comma delimited)")
	pflag.IntVar(&fieldIndexCount, "field-index-count", 0, "Number of indexed fields (-1 for all)")

	pflag.String("write-profile", "", "File to output CPU/memory profile to")
	pflag.String("write-replication-stats", "", "File to output replication stats to")
	pflag.Bool("create-metrics-table", true, "Drops existing and creates new metrics table. Can be used for both regular and hypertable")

	pflag.BoolVar(&forceTextFormat, "force-text-format", false, "Send/receive data in text format")
	pflag.BoolVar(&useCopy, "use-copy", true, "Perform inserts using COPY")
	pflag.IntVar(&replicationFactor, "replication-factor", 0, "To create distributed hypertable use use replication-factor >= 1")

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
