// tsbs_load_timescaledb loads a TimescaleDB instance with data from stdin.
//
// If the database exists beforehand, it will be *DROPPED*.
package main

import (
	"bufio"
	"fmt"
	"log"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/load"
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

	timeIndex          bool
	timePartitionIndex bool
	partitionIndex     bool
	fieldIndex         string
	fieldIndexCount    int

	profileFile          string
	replicationStatsFile string

	createMetricsTable bool
	forceTextFormat    bool
	tagColumnTypes     []string
)

type insertData struct {
	tags   string
	fields string
}

// Global vars
var loader *load.BenchmarkRunner

// allows for testing
var fatal = log.Fatalf

// Parse args:
func init() {
	var config load.BenchmarkRunnerConfig
	config.AddToFlagSet(pflag.CommandLine)

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

	pflag.Bool("time-index", true, "Whether to build an index on the time dimension")
	pflag.Bool("time-partition-index", false, "Whether to build an index on the time dimension, compounded with partition")
	pflag.Bool("partition-index", true, "Whether to build an index on the partition key")
	pflag.String("field-index", valueTimeIdx, "index types for tags (comma delimited)")
	pflag.Int("field-index-count", 0, "Number of indexed fields (-1 for all)")

	pflag.String("write-profile", "", "File to output CPU/memory profile to")
	pflag.String("write-replication-stats", "", "File to output replication stats to")
	pflag.Bool("create-metrics-table", true, "Drops existing and creates new metrics table. Can be used for both regular and hypertable")

	pflag.Bool("force-text-format", false, "Send/receive data in text format")

	pflag.Parse()

	err := utils.SetupConfigFile()

	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}

	if err := viper.Unmarshal(&config); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}

	postgresConnect = viper.GetString("postgres")
	host = viper.GetString("host")
	port = viper.GetString("port")
	user = viper.GetString("user")
	pass = viper.GetString("pass")
	connDB = viper.GetString("admin-db-name")
	logBatches = viper.GetBool("log-batches")

	useHypertable = viper.GetBool("use-hypertable")
	useJSON = viper.GetBool("use-jsonb-tags")
	inTableTag = viper.GetBool("in-table-partition-tag")
	hashWorkers = viper.GetBool("hash-workers")

	numberPartitions = viper.GetInt("partitions")
	chunkTime = viper.GetDuration("chunk-time")

	timeIndex = viper.GetBool("time-index")
	timePartitionIndex = viper.GetBool("time-partition-index")
	partitionIndex = viper.GetBool("partition-index")
	fieldIndex = viper.GetString("field-index")
	fieldIndexCount = viper.GetInt("field-index-count")

	profileFile = viper.GetString("write-profile")
	replicationStatsFile = viper.GetString("write-replication-stats")
	createMetricsTable = viper.GetBool("create-metrics-table")

	forceTextFormat = viper.GetBool("force-text-format")

	loader = load.GetBenchmarkRunner(config)
}

type benchmark struct{}

func (b *benchmark) GetPointDecoder(br *bufio.Reader) load.PointDecoder {
	return &decoder{scanner: bufio.NewScanner(br)}
}

func (b *benchmark) GetBatchFactory() load.BatchFactory {
	return &factory{}
}

func (b *benchmark) GetPointIndexer(maxPartitions uint) load.PointIndexer {
	if hashWorkers {
		return &hostnameIndexer{partitions: maxPartitions}
	}
	return &load.ConstantIndexer{}
}

func (b *benchmark) GetProcessor() load.Processor {
	return &processor{}
}

func (b *benchmark) GetDBCreator() load.DBCreator {
	return &dbCreator{
		br:      loader.GetBufferedReader(),
		connStr: getConnectString(),
		connDB:  connDB,
	}
}

func main() {
	if forceTextFormat {
		driver = pqDriver
	} else {
		driver = pgxDriver
	}
	// If specified, generate a performance profile
	if len(profileFile) > 0 {
		go profileCPUAndMem(profileFile)
	}

	var replicationStatsWaitGroup sync.WaitGroup
	if len(replicationStatsFile) > 0 {
		go OutputReplicationStats(getConnectString(), replicationStatsFile, &replicationStatsWaitGroup)
	}

	if hashWorkers {
		loader.RunBenchmark(&benchmark{}, load.WorkerPerQueue)
	} else {
		loader.RunBenchmark(&benchmark{}, load.SingleQueue)
	}

	if len(replicationStatsFile) > 0 {
		replicationStatsWaitGroup.Wait()
	}
}

func getConnectString() string {
	// User might be passing in host=hostname the connect string out of habit which may override the
	// multi host configuration. Same for dbname= and user=. This sanitizes that.
	re := regexp.MustCompile(`(host|dbname|user)=\S*\b`)
	connectString := strings.TrimSpace(re.ReplaceAllString(postgresConnect, ""))
	connectString = fmt.Sprintf("host=%s dbname=%s user=%s %s", host, loader.DatabaseName(), user, connectString)

	// For optional parameters, ensure they exist then interpolate them into the connectString
	if len(port) > 0 {
		connectString = fmt.Sprintf("%s port=%s", connectString, port)
	}
	if len(pass) > 0 {
		connectString = fmt.Sprintf("%s password=%s", connectString, pass)
	}

	if forceTextFormat {
		// we assume we're using pq driver
		connectString = fmt.Sprintf("%s disable_prepared_binary_result=yes binary_parameters=no", connectString)
	}

	return connectString
}
