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

	"bitbucket.org/440-labs/tsbs/load"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
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
)

type insertData struct {
	tags   string
	fields string
}

// Global vars
var (
	loader    *load.BenchmarkRunner
	tableCols map[string][]string
)

// allows for testing
var fatal = log.Fatalf

// Parse args:
func init() {
	loader = load.GetBenchmarkRunner()

	flag.StringVar(&postgresConnect, "postgres", "sslmode=disable", "PostgreSQL connection string")
	flag.StringVar(&host, "host", "localhost", "Hostname of TimescaleDB (PostgreSQL) instance")
	flag.StringVar(&user, "user", "postgres", "User to connect to PostgreSQL as")

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

	flag.Parse()
	tableCols = make(map[string][]string)
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

func main() {
	br := loader.GetBufferedReader()
	tags, cols := readDataHeader(br)

	if loader.DoLoad() && loader.DoInit() {
		initDB(loader.DatabaseName(), tags, cols)
	}

	/* If specified, generate a performance profile */
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

func readDataHeader(br *bufio.Reader) (tags, cols string) {
	// First three lines are header, with the first line containing the tags
	// and their names, the second line containing the column names, and
	// third line being blank to separate from the data
	for i := 0; i < 3; i++ {
		var err error
		var empty string
		if i == 0 {
			tags, err = br.ReadString('\n')
			tags = strings.TrimSpace(tags)
		} else if i == 1 {
			cols, err = br.ReadString('\n')
			cols = strings.TrimSpace(cols)
		} else {
			empty, err = br.ReadString('\n')
			empty = strings.TrimSpace(empty)
			if len(empty) > 0 {
				fatal("input has wrong header format: third line is not blank")
			}
		}
		if err != nil {
			fatal("input has wrong header format: %v", err)
		}
	}
	return tags, cols
}

func getConnectString() string {
	// User might be passing in host=hostname the connect string out of habit which may override the
	// multi host configuration. Same for dbname= and user=. This sanitizes that.
	re := regexp.MustCompile(`(host|dbname|user)=\S*\b`)
	connectString := strings.TrimSpace(re.ReplaceAllString(postgresConnect, ""))

	return fmt.Sprintf("host=%s dbname=%s user=%s %s", host, loader.DatabaseName(), user, connectString)
}

func createTagsTable(db *sqlx.DB, tags []string) {
	if useJSON {
		db.MustExec("CREATE TABLE tags(id SERIAL PRIMARY KEY, tagset JSONB)")
		db.MustExec("CREATE UNIQUE INDEX uniq1 ON tags(tagset)")
		db.MustExec("CREATE INDEX idxginp ON tags USING gin (tagset jsonb_path_ops);")
	} else {
		cols := strings.Join(tags, " TEXT, ")
		cols += " TEXT"
		db.MustExec(fmt.Sprintf("CREATE TABLE tags(id SERIAL PRIMARY KEY, %s)", cols))
		db.MustExec(fmt.Sprintf("CREATE UNIQUE INDEX uniq1 ON tags(%s)", strings.Join(tags, ",")))
		db.MustExec(fmt.Sprintf("CREATE INDEX ON tags(%s)", tags[0]))
	}
}

func dropExistingDB(dbName string) {
	// Need to connect to user's database in order to drop/create db-name database
	re := regexp.MustCompile(`(dbname)=\S*\b`)
	connectString := re.ReplaceAllString(getConnectString(), "")

	db := sqlx.MustConnect(dbType, connectString)
	db.MustExec("DROP DATABASE IF EXISTS " + dbName)
	db.MustExec("CREATE DATABASE " + dbName)
	db.Close()
}

func getCreateIndexOnFieldCmds(hypertable, field, idxType string) []string {
	ret := []string{}
	for _, idx := range strings.Split(idxType, ",") {
		if idx == "" {
			continue
		}

		indexDef := ""
		if idx == timeValueIdx {
			indexDef = fmt.Sprintf("(time DESC, %s)", field)
		} else if idx == valueTimeIdx {
			indexDef = fmt.Sprintf("(%s, time DESC)", field)
		} else {
			fatal("Unknown index type %v", idx)
		}

		ret = append(ret, fmt.Sprintf("CREATE INDEX ON %s %s", hypertable, indexDef))
	}
	return ret
}

func initDB(dbName, tags, cols string) {
	dropExistingDB(dbName)

	dbBench := sqlx.MustConnect(dbType, getConnectString())
	defer dbBench.Close()

	parts := strings.Split(strings.TrimSpace(tags), ",")
	if parts[0] != "tags" {
		log.Fatalf("input header in wrong format. got '%s', expected 'tags'", parts[0])
	}
	createTagsTable(dbBench, parts[1:])
	tableCols["tags"] = parts[1:]

	parts = strings.Split(strings.TrimSpace(cols), ",")
	hypertable := parts[0]
	partitioningField := tableCols["tags"][0]
	tableCols[hypertable] = parts[1:]

	psuedoCols := []string{}
	if inTableTag {
		psuedoCols = append(psuedoCols, partitioningField)
	}

	fieldDef := []string{}
	indexes := []string{}
	psuedoCols = append(psuedoCols, parts[1:]...)
	extraCols := 0 // set to 1 when hostname is kept in-table
	for idx, field := range psuedoCols {
		if len(field) == 0 {
			continue
		}
		fieldType := "DOUBLE PRECISION"
		idxType := fieldIndex
		if inTableTag && idx == 0 {
			fieldType = "TEXT"
			idxType = ""
			extraCols = 1
		}

		fieldDef = append(fieldDef, fmt.Sprintf("%s %s", field, fieldType))
		if fieldIndexCount == -1 || idx < (fieldIndexCount+extraCols) {
			indexes = append(indexes, getCreateIndexOnFieldCmds(hypertable, field, idxType)...)
		}
	}
	dbBench.MustExec(fmt.Sprintf("CREATE TABLE %s (time timestamptz, tags_id integer, %s)", hypertable, strings.Join(fieldDef, ",")))
	if partitionIndex {
		dbBench.MustExec(fmt.Sprintf("CREATE INDEX ON %s(tags_id, \"time\" DESC)", hypertable))
	}

	// Only allow one or the other, it's probably never right to have both.
	// Experimentation suggests (so far) that for 100k devices it is better to
	// use --time-partition-index for reduced index lock contention.
	if timePartitionIndex {
		dbBench.MustExec(fmt.Sprintf("CREATE INDEX ON %s(\"time\" DESC, tags_id)", hypertable))
	} else if timeIndex {
		dbBench.MustExec(fmt.Sprintf("CREATE INDEX ON %s(\"time\" DESC)", hypertable))
	}

	for _, idxDef := range indexes {
		dbBench.MustExec(idxDef)
	}

	if useHypertable {
		dbBench.MustExec("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE")
		dbBench.MustExec(
			fmt.Sprintf("SELECT create_hypertable('%s'::regclass, 'time'::name, partitioning_column => '%s'::name, number_partitions => %v::smallint, chunk_time_interval => %d, create_default_indexes=>FALSE)",
				hypertable, "tags_id", numberPartitions, chunkTime.Nanoseconds()/1000))
	}
}
