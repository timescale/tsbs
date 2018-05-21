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
	"strconv"
	"strings"
	"sync"
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/load"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

const (
	dbType = "postgres"
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

	numberPartitions int
	chunkTime        time.Duration

	timeIndex       bool
	partitionIndex  bool
	fieldIndex      string
	fieldIndexCount int

	profileFile          string
	replicationStatsFile string
)

type insertData struct {
	tags   string
	fields string
}

// Global vars
var (
	loader *load.BenchmarkRunner

	tableCols map[string][]string
)

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

	flag.IntVar(&numberPartitions, "partitions", 1, "Number of patitions")
	flag.DurationVar(&chunkTime, "chunk-time", 12*time.Hour, "Duration that each chunk should represent, e.g., 12h")

	flag.BoolVar(&timeIndex, "time-index", true, "Whether to build an index on the time dimension")
	flag.BoolVar(&partitionIndex, "partition-index", true, "Whether to build an index on the partition key")
	flag.StringVar(&fieldIndex, "field-index", "VALUE-TIME", "index types for tags (comma deliminated)")
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

func (b *benchmark) GetPointIndexer(_ uint) load.PointIndexer {
	return &load.ConstantIndexer{}
}

func (b *benchmark) GetProcessor() load.Processor {
	return &processor{}
}

func main() {
	br := loader.GetBufferedReader()
	var tags string
	var cols string
	var err error
	// First three lines are header, with the first line containing the tags
	// and their names, the second line containing the column names, and
	// third line being blank to separate from the data
	for i := 0; i < 3; i++ {
		if i == 0 {
			tags, err = br.ReadString('\n')
		} else if i == 1 {
			cols, err = br.ReadString('\n')
		} else {
			_, err = br.ReadString('\n')
		}
		if err != nil {
			log.Fatalf("input has wrong header format: %v", err)
		}
	}

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

	loader.RunBenchmark(&benchmark{}, load.SingleQueue)

	if len(replicationStatsFile) > 0 {
		replicationStatsWaitGroup.Wait()
	}
}

func getConnectString() string {
	// User might be passing in host=hostname the connect string out of habit which may override the
	// multi host configuration. Same for dbname= and user=. This sanitizes that.
	re := regexp.MustCompile(`(host|dbname|user)=\S*\b`)
	connectString := re.ReplaceAllString(postgresConnect, "")

	return fmt.Sprintf("host=%s dbname=%s user=%s %s", host, loader.DatabaseName(), user, connectString)
}

func insertTags(db *sqlx.DB, tagRows [][]string, returnResults bool) map[string]int64 {
	tagCols := tableCols["tags"]
	cols := tagCols
	values := make([]string, 0)
	if useJSON {
		cols = []string{"tagset"}
		for _, row := range tagRows {
			json := "('{"
			for i, k := range tagCols {
				if i != 0 {
					json += ","
				}
				json += fmt.Sprintf("\"%s\": \"%s\"", k, row[i])
			}
			json += "}')"
			values = append(values, json)
		}
	} else {
		for _, val := range tagRows {
			values = append(values, fmt.Sprintf("('%s')", strings.Join(val[:10], "','")))
		}
	}
	tx := db.MustBegin()
	defer tx.Commit()

	res, err := tx.Query(fmt.Sprintf(`INSERT INTO tags(%s) VALUES %s ON CONFLICT DO NOTHING RETURNING *`, strings.Join(cols, ","), strings.Join(values, ",")))
	if err != nil {
		panic(err)
	}

	// Results will be used to make a Golang index for faster inserts
	if returnResults {
		resCols, _ := res.Columns()
		resVals := make([]interface{}, len(resCols))
		resValsPtrs := make([]interface{}, len(resCols))
		for i := range resVals {
			resValsPtrs[i] = &resVals[i]
		}
		ret := make(map[string]int64)
		for res.Next() {
			err = res.Scan(resValsPtrs...)
			if err != nil {
				panic(err)
			}
			ret[fmt.Sprintf("%v", resVals[1])] = resVals[0].(int64)
		}
		res.Close()
		return ret
	}
	return nil
}

// 1 - tag cols JOIN w/ ,
// 2 - metric cols JOIN w/ ,
// 3 - Each row tags + metrics joined
// 4 - hypertable name
// 5 - partitionKey
// 6 - same as 2
// 7 - same as 5
// 8 - same as 2
// 9 - same as 1
var insertFmt2 = `INSERT INTO %s(time,tags_id,%s,%s)
SELECT time,id,%s,%s
FROM (VALUES %s) as temp(%s,time,%s)
JOIN tags USING (%s);
`

var calledOnce = false

// TODO - Needs work to work without partition tag being in table
func processSplit(db *sqlx.DB, hypertable string, rows []*insertData) int64 {
	tagCols := strings.Join(tableCols["tags"], ",")
	partitionKey := tableCols["tags"][0]

	hypertableCols := strings.Join(tableCols[hypertable], ",")

	tagRows := make([][]string, 0, len(rows))
	dataRows := make([]string, 0, len(rows))
	ret := int64(0)
	for _, data := range rows {
		tags := strings.Split(data.tags, ",")
		metrics := strings.Split(data.fields, ",")

		ret += int64(len(metrics) - 1) // 1 field is timestamp
		r := "("
		// TODO -- support more than 10 common tags
		for _, t := range tags[:10] {
			r += fmt.Sprintf("'%s',", t)
		}
		for ind, value := range metrics {
			if ind == 0 {
				timeInt, err := strconv.ParseInt(value, 10, 64)
				if err != nil {
					panic(err)
				}
				secs := timeInt / 1e9
				r += fmt.Sprintf("'%s'::timestamptz", time.Unix(secs, timeInt%1e9).Format("2006-01-02 15:04:05.999999 -7:00"))
			} else {
				r += fmt.Sprintf(", %v", value)
			}
		}
		r += ")"
		dataRows = append(dataRows, r)
		tagRows = append(tagRows, tags[:10]) //fmt.Sprintf("('%s')", strings.Join(tags[:10], "','")))
	}

	if !calledOnce {
		insertTags(db, tagRows, false)
		calledOnce = true
	}

	tx := db.MustBegin()
	_ = tx.MustExec(fmt.Sprintf(insertFmt2, hypertable, partitionKey, hypertableCols, partitionKey, hypertableCols, strings.Join(dataRows, ","), tagCols, hypertableCols, tagCols))

	err := tx.Commit()
	if err != nil {
		panic(err)
	}

	return ret
}

var csi = make(map[string]int64)
var mutex = &sync.RWMutex{}
var insertFmt3 = `INSERT INTO %s(time,tags_id,%s%s) VALUES %s`

func processCSI(db *sqlx.DB, hypertable string, rows []*insertData) uint64 {
	partitionKey := ""
	if inTableTag {
		partitionKey = tableCols["tags"][0] + ","
	}

	hypertableCols := strings.Join(tableCols[hypertable], ",")

	tagRows := make([][]string, 0, len(rows))
	dataRows := make([]string, 0, len(rows))
	ret := uint64(0)
	for _, data := range rows {
		tags := strings.Split(data.tags, ",")
		metrics := strings.Split(data.fields, ",")
		ret += uint64(len(metrics) - 1) // 1 field is timestamp

		timeInt, err := strconv.ParseInt(metrics[0], 10, 64)
		if err != nil {
			panic(err)
		}
		ts := time.Unix(0, timeInt).Format("2006-01-02 15:04:05.999999 -0700")

		// The last arguments in these sprintf's may seem a bit confusing at first, but
		// it does work. We want each value to be surrounded by single quotes (' '), and
		// to be separated by a comma. That means we strings.Join them with "', '", which
		// leaves the first value without a preceding ' and the last with out a trailing ',
		// therefore we put the %s returned by the Join inside of '' to solve the problem
		var r string
		if inTableTag {
			r = fmt.Sprintf("('%s','[REPLACE_CSI]', '%s', '%s')", ts, tags[0], strings.Join(metrics[1:], "', '"))
		} else {
			r = fmt.Sprintf("('%s', '[REPLACE_CSI]', '%s')", ts, strings.Join(metrics[1:], "', '"))
		}

		dataRows = append(dataRows, r)
		tagRows = append(tagRows, tags[:10])
	}

	// Check if any of these tags has yet to be inserted
	newTags := make([][]string, 0, len(rows))
	mutex.RLock()
	for _, cols := range tagRows {
		if _, ok := csi[cols[0]]; !ok {
			newTags = append(newTags, cols)
		}
	}
	mutex.RUnlock()
	if len(newTags) > 0 {
		mutex.Lock()
		res := insertTags(db, newTags, true)
		for k, v := range res {
			csi[k] = v
		}
		mutex.Unlock()
	}

	mutex.RLock()
	for i, r := range dataRows {
		// TODO -- support more than 10 common tags
		tagKey := tagRows[i][0]
		dataRows[i] = strings.Replace(r, "[REPLACE_CSI]", strconv.FormatInt(csi[tagKey], 10), 1)
	}
	mutex.RUnlock()
	tx := db.MustBegin()
	_ = tx.MustExec(fmt.Sprintf(insertFmt3, hypertable, partitionKey, hypertableCols, strings.Join(dataRows, ",")))

	err := tx.Commit()
	if err != nil {
		panic(err)
	}

	return ret
}

type processor struct {
	db *sqlx.DB
}

func (p *processor) Init(workerNum int, doLoad bool) {
	if doLoad {
		p.db = sqlx.MustConnect(dbType, getConnectString())
	}
}

func (p *processor) Close(doLoad bool) {
	if doLoad {
		p.db.Close()
	}
}

func (p *processor) ProcessBatch(b load.Batch, doLoad bool) (uint64, uint64) {
	batches := b.(*hypertableArr)
	rowCnt := 0
	metricCnt := uint64(0)
	for hypertable, rows := range batches.m {
		rowCnt += len(rows)
		if doLoad {
			start := time.Now()
			metricCnt += processCSI(p.db, hypertable, rows)
			//metricCountWorker := processSplit(db, hypertable, rows)

			if logBatches {
				now := time.Now()
				took := now.Sub(start)
				batchSize := len(rows)
				fmt.Printf("BATCH: batchsize %d row rate %f/sec (took %v)\n", batchSize, float64(batchSize)/float64(took.Seconds()), took)
			}
		}
	}
	batches.m = map[string][]*insertData{}
	batches.cnt = 0
	return metricCnt, uint64(rowCnt)
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

func initDB(dbName, tags, cols string) {
	// Need to connect to user's database in order to drop/create db-name database
	re := regexp.MustCompile(`(dbname)=\S*\b`)
	connectString := re.ReplaceAllString(getConnectString(), "")

	db := sqlx.MustConnect(dbType, connectString)
	db.MustExec("DROP DATABASE IF EXISTS " + dbName)
	db.MustExec("CREATE DATABASE " + dbName)
	db.Close()

	dbBench := sqlx.MustConnect(dbType, getConnectString())
	defer dbBench.Close()

	if useHypertable {
		dbBench.MustExec("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE")
	}

	parts := strings.Split(strings.TrimSpace(tags), ",")
	if parts[0] != "tags" {
		log.Fatalf("input header in wrong format. got '%s', expected 'tags'", parts[0])
	}
	createTagsTable(dbBench, parts[1:])
	tableCols["tags"] = parts[1:]

	parts = strings.Split(strings.TrimSpace(cols), ",")
	hypertable := parts[0]
	partitioningField := tableCols["tags"][0]
	fieldDef := []string{}
	indexes := []string{}
	tableCols[hypertable] = parts[1:]

	psuedoCols := []string{}
	if inTableTag {
		psuedoCols = append(psuedoCols, partitioningField)
	}
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
			for _, idx := range strings.Split(idxType, ",") {
				indexDef := ""
				if idx == "TIME-VALUE" {
					indexDef = fmt.Sprintf("(time DESC, %s)", field)
				} else if idx == "VALUE-TIME" {
					indexDef = fmt.Sprintf("(%s,time DESC)", field)
				} else if idx != "" {
					panic(fmt.Sprintf("Unknown index type %v", idx))
				}

				if idx != "" {
					indexes = append(indexes, fmt.Sprintf("CREATE INDEX ON %s %s", hypertable, indexDef))
				}
			}
		}
	}
	dbBench.MustExec(fmt.Sprintf("CREATE TABLE %s (time timestamptz, tags_id integer, %s)", hypertable, strings.Join(fieldDef, ",")))
	if partitionIndex {
		dbBench.MustExec(fmt.Sprintf("CREATE INDEX ON %s(tags_id, \"time\" DESC)", hypertable))
	}
	if timeIndex {
		dbBench.MustExec(fmt.Sprintf("CREATE INDEX ON %s(\"time\" DESC)", hypertable))
	}

	for _, idxDef := range indexes {
		dbBench.MustExec(idxDef)
	}

	if useHypertable {
		dbBench.MustExec(
			fmt.Sprintf("SELECT create_hypertable('%s'::regclass, 'time'::name, partitioning_column => '%s'::name, number_partitions => %v::smallint, chunk_time_interval => %d, create_default_indexes=>FALSE)",
				hypertable, "tags_id", numberPartitions, chunkTime.Nanoseconds()/1000))
	}
}
