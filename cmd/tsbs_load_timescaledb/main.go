// tsbs_load_timescaledb loads a TimescaleDB instance with data from stdin.
//
// If the database exists beforehand, it will be *DROPPED*.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"regexp"
	"strings"
	"sync"
	"time"

	_ "github.com/lib/pq"
	"github.com/timescale/tsbs/load"
)

const (
	dbType       = "postgres"
	timeValueIdx = "TIME-VALUE"
	valueTimeIdx = "VALUE-TIME"
)

// Program option vars:
var (
	postgresConnect string
	host            string
	user            string
	pass            string
	port            string
	connDB          string

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
	loader = load.GetBenchmarkRunner()

	flag.StringVar(&postgresConnect, "postgres", "sslmode=disable", "PostgreSQL connection string")
	flag.StringVar(&host, "host", "localhost", "Hostname of TimescaleDB (PostgreSQL) instance")
	flag.StringVar(&port, "port", "5432", "Which port to connect to on the database host")
	flag.StringVar(&user, "user", "postgres", "User to connect to PostgreSQL as")
	flag.StringVar(&pass, "pass", "", "Password for user connecting to PostgreSQL (leave blank if not password protected)")
	flag.StringVar(&connDB, "admin-db-name", user, "Database to connect to in order to create additional benchmark databases.\n"+
		"By default this is the same as the `user` (i.e., `postgres` if neither is set),\n"+
		"but sometimes a user does not have its own database.")

	flag.BoolVar(&logBatches, "log-batches", false, "Whether to time individual batches.")

	flag.BoolVar(&useHypertable, "use-hypertable", true, "Whether to make the table a hypertable. Set this flag to false to check input write speed against regular PostgreSQL.")
	flag.BoolVar(&useJSON, "use-jsonb-tags", false, "Whether tags should be stored as JSONB (instead of a separate table with schema)")
	flag.BoolVar(&inTableTag, "in-table-partition-tag", false, "Whether the partition key (e.g. hostname) should also be in the metrics hypertable")
	// TODO - This flag could potentially be done as a string/enum with other options besides no-hash, round-robin, etc
	flag.BoolVar(&hashWorkers, "hash-workers", false, "Whether to consistently hash insert data to the same workers (i.e., the data for a particular host always goes to the same worker)")

	flag.IntVar(&numberPartitions, "partitions", 1, "Number of patitions")
	flag.DurationVar(&chunkTime, "chunk-time", 12*time.Hour, "Duration that each chunk should represent, e.g., 12h")

	flag.BoolVar(&timeIndex, "time-index", true, "Whether to build an index on the time dimension")
	flag.BoolVar(&timePartitionIndex, "time-partition-index", false, "Whether to build an index on the time dimension, compounded with partition")
	flag.BoolVar(&partitionIndex, "partition-index", true, "Whether to build an index on the partition key")
	flag.StringVar(&fieldIndex, "field-index", valueTimeIdx, "index types for tags (comma deliminated)")
	flag.IntVar(&fieldIndexCount, "field-index-count", 0, "Number of indexed fields (-1 for all)")

	flag.StringVar(&profileFile, "write-profile", "", "File to output CPU/memory profile to")
	flag.StringVar(&replicationStatsFile, "write-replication-stats", "", "File to output replication stats to")
	flag.BoolVar(&createMetricsTable, "create-metrics-table", true, "Drops existing and creates new metrics table. Can be used for both regular and hypertable")

	flag.Parse()
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

	return connectString
}
