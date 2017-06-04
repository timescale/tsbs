// bulk_load_cassandra loads a Cassandra daemon with data from stdin.
//
// The caller is responsible for assuring that the database is empty before
// bulk load.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

// Program option vars:
var (
	postgresConnect  string
	databaseName     string
	workers          int
	batchSize        int
	doLoad           bool
	useHypertable    bool
	logBatches       bool
	tagIndex         string
	fieldIndex       string
	fieldIndexCount  int
	reportingPeriod  time.Duration
	numberPartitions int
	chunkTime        time.Duration
	columnCount      int64
	rowCount         int64
	useJSON          bool

	tableCols map[string][]string
)

type insertData struct {
	tags   string
	fields string
}

type hypertableBatch struct {
	hypertable string
	rows       []insertData
}

// Global vars
var (
	batchChan    chan *hypertableBatch
	inputDone    chan struct{}
	workersGroup sync.WaitGroup
)

// Parse args:
func init() {
	flag.StringVar(&postgresConnect, "postgres", "host=postgres user=postgres sslmode=disable", "Postgres connection url")
	flag.StringVar(&databaseName, "db-name", "benchmark", "Name of database to store data")

	flag.IntVar(&batchSize, "batch-size", 10000, "Batch size (input items).")
	flag.IntVar(&workers, "workers", 1, "Number of parallel requests to make.")

	flag.BoolVar(&doLoad, "do-load", true, "Whether to write data. Set this flag to false to check input read speed.")
	flag.BoolVar(&useHypertable, "use-hypertable", true, "Whether to make the table a hypertable. Set this flag to false to check input write speed and how much the insert logic slows things down.")
	flag.BoolVar(&logBatches, "log-batches", false, "Whether to time individual batches.")
	flag.BoolVar(&useJSON, "jsonb-tags", false, "Whether tags should be stored as JSONB")

	flag.StringVar(&tagIndex, "tag-index", "VALUE-TIME,TIME-VALUE", "index types for tags (comma deliminated)")
	flag.StringVar(&fieldIndex, "field-index", "TIME-VALUE", "index types for tags (comma deliminated)")
	flag.IntVar(&fieldIndexCount, "field-index-count", -1, "Number of indexed fields (-1 for all)")
	flag.IntVar(&numberPartitions, "number_partitions", 1, "Number of patitions")
	flag.DurationVar(&chunkTime, "chunk-time", 8*time.Hour, "Duration that each chunk should represent, e.g., 6h")
	flag.DurationVar(&reportingPeriod, "reporting-period", time.Second, "Period to report stats")

	flag.Parse()
	tableCols = make(map[string][]string)
}

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	if doLoad {
		initBenchmarkDB(postgresConnect, scanner)
	} else {
		//read the header
		for scanner.Scan() {
			if len(scanner.Bytes()) == 0 {
				break
			}
		}
	}

	batchChan = make(chan *hypertableBatch, workers)
	inputDone = make(chan struct{})

	for i := 0; i < workers; i++ {
		workersGroup.Add(1)
		go processBatches(postgresConnect)
	}

	go report(int(reportingPeriod.Nanoseconds() / 1e6))

	start := time.Now()
	rowsRead := scan(batchSize, scanner)

	<-inputDone
	close(batchChan)
	workersGroup.Wait()
	end := time.Now()
	took := end.Sub(start)
	columnsRead := columnCount
	rowRate := float64(rowsRead) / float64(took.Seconds())
	columnRate := float64(columnsRead) / float64(took.Seconds())

	fmt.Printf("loaded %d rows in %fsec with %d workers (mean rate %f/sec)\n", rowsRead, took.Seconds(), workers, rowRate)
	fmt.Printf("loaded %d columns in %fsec with %d workers (mean rate %f/sec)\n", columnsRead, took.Seconds(), workers, columnRate)
}

func getConnectString() string {
	return postgresConnect + " dbname=" + databaseName
}

func report(periodMs int) {
	c := time.Tick(time.Duration(periodMs) * time.Millisecond)
	start := time.Now()
	prevTime := start
	prevColCount := int64(0)
	prevRowCount := int64(0)

	for now := range c {
		colCount := atomic.LoadInt64(&columnCount)
		rowCount := atomic.LoadInt64(&rowCount)

		took := now.Sub(prevTime)
		colrate := float64(colCount-prevColCount) / float64(took.Seconds())
		rowrate := float64(rowCount-prevRowCount) / float64(took.Seconds())
		overallRowrate := float64(rowCount) / float64(now.Sub(start).Seconds())

		fmt.Printf("REPORT: time %d col rate %f/sec row rate %f/sec (period) %f/sec (total) total rows %E\n", now.Unix(), colrate, rowrate, overallRowrate, float64(rowCount))

		prevColCount = colCount
		prevRowCount = rowCount
		prevTime = now
	}

}

// scan reads lines from stdin. It expects input in the TimescaleDB format,
// which is a pair of lines: the first begins with the prefix 'tags' and is
// CSV of tags, the second is a list of fields
func scan(itemsPerBatch int, scanner *bufio.Scanner) int64 {
	batch := make(map[string][]insertData) // hypertable => copy lines
	linesRead := int64(0)
	n := 0

	data := insertData{}
	for scanner.Scan() {
		linesRead++

		parts := strings.SplitN(scanner.Text(), ",", 2) // prefix & then rest of line
		prefix := parts[0]
		if prefix == "tags" {
			data.tags = parts[1]
			continue
		} else {
			data.fields = parts[1]
			batch[prefix] = append(batch[prefix], data)
		}

		n++
		if n >= itemsPerBatch {
			for hypertable, rows := range batch {
				batchChan <- &hypertableBatch{hypertable, rows}
			}

			batch = make(map[string][]insertData)
			n = 0
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("Error reading input: %s", err.Error())
	}

	// Finished reading input, make sure last batch goes out.
	if n > 0 {
		for hypertable, rows := range batch {
			batchChan <- &hypertableBatch{hypertable, rows}
		}
	}

	// Closing inputDone signals to the application that we've read everything and can now shut down.
	close(inputDone)

	return linesRead / 2
}

func insertTags(tx *sqlx.Tx, tagRows [][]string, returnResults bool) map[string]int64 {
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
		i := 0
		for res.Next() {
			err = res.Scan(resValsPtrs...)
			if err != nil {
				panic(err)
			}
			ret[strings.Join(tagRows[i], ",")] = resVals[0].(int64)
			i++
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

func processSplit(db *sqlx.DB, hypertableBatch *hypertableBatch) int64 {
	tagCols := strings.Join(tableCols["tags"], ",")
	partitionKey := tableCols["tags"][0]

	hypertable := hypertableBatch.hypertable
	hypertableCols := strings.Join(tableCols[hypertable], ",")

	tagRows := make([][]string, 0, len(hypertableBatch.rows))
	dataRows := make([]string, 0, len(hypertableBatch.rows))
	tx := db.MustBegin()
	ret := int64(0)
	//start := time.Now()
	for _, data := range hypertableBatch.rows {
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

	//fmt.Printf("t1: %v\n", time.Now().Sub(start))
	if !calledOnce {
		insertTags(tx, tagRows, false)
		calledOnce = true
	}
	//fmt.Printf(insertFmt2, hypertable, partitionKey, hypertableCols, partitionKey, hypertableCols, strings.Join(dataRows[:2], ","), tagCols, hypertableCols, tagCols)
	_ = tx.MustExec(fmt.Sprintf(insertFmt2, hypertable, partitionKey, hypertableCols, partitionKey, hypertableCols, strings.Join(dataRows, ","), tagCols, hypertableCols, tagCols))

	err := tx.Commit()
	if err != nil {
		panic(err)
	}
	//fmt.Printf("t2: %v\n", time.Now().Sub(start))

	return ret
}

var csi = make(map[string]int64)
var csiCnt = int64(0)
var mutex = &sync.RWMutex{}
var insertFmt3 = `INSERT INTO %s(time,tags_id,%s,%s) VALUES %s`

func processCSI(db *sqlx.DB, hypertableBatch *hypertableBatch) int64 {
	//tagCols := strings.Join(tableCols["tags"], ",")
	partitionKey := tableCols["tags"][0]

	hypertable := hypertableBatch.hypertable
	hypertableCols := strings.Join(tableCols[hypertable], ",")

	tagRows := make([][]string, 0, len(hypertableBatch.rows))
	dataRows := make([]string, 0, len(hypertableBatch.rows))
	tx := db.MustBegin()
	ret := int64(0)
	//start := time.Now()
	for _, data := range hypertableBatch.rows {
		tags := strings.Split(data.tags, ",")
		metrics := strings.Split(data.fields, ",")

		ret += int64(len(metrics) - 1) // 1 field is timestamp
		r := "("
		for ind, value := range metrics {
			if ind == 0 {
				timeInt, err := strconv.ParseInt(value, 10, 64)
				if err != nil {
					panic(err)
				}
				secs := timeInt / 1e9
				r += fmt.Sprintf("'%s',", time.Unix(secs, timeInt%1e9).Format("2006-01-02 15:04:05.999999 -7:00"))
				r += fmt.Sprintf("'[REPLACE_CSI]',")
				r += fmt.Sprintf("'%s'", tags[0])

			} else {
				r += fmt.Sprintf(", '%v'", value)
			}
		}
		r += ")"
		dataRows = append(dataRows, r)
		tagRows = append(tagRows, tags[:10])
	}

	// Check if any of these tags has yet to be inserted
	mutex.RLock()
	insert := false
	for _, cols := range tagRows {
		// TODO - Might be more performant to just insert those that haven't?
		if _, ok := csi[strings.Join(cols, ",")]; !ok {
			insert = true
			break
		}
	}
	mutex.RUnlock()

	if insert {
		res := insertTags(tx, tagRows, true)
		mutex.Lock()
		for k, v := range res {
			csi[k] = v
		}
		mutex.Unlock()
	}

	mutex.RLock()
	for i, r := range dataRows {
		// TODO -- support more than 10 common tags
		tagKey := strings.Join(tagRows[i][:10], ",")
		dataRows[i] = strings.Replace(r, "[REPLACE_CSI]", strconv.FormatInt(csi[tagKey], 10), 1)
	}
	mutex.RUnlock()
	//fmt.Printf(insertFmt3, partitionKey, hypertableCols, strings.Join(dataRows, ","), hypertable, partitionKey, hypertableCols, partitionKey, hypertableCols)
	_ = tx.MustExec(fmt.Sprintf(insertFmt3, hypertable, partitionKey, hypertableCols, strings.Join(dataRows, ",")))

	err := tx.Commit()
	if err != nil {
		panic(err)
	}
	//fmt.Printf("t2: %v\n", time.Now().Sub(start))

	return ret
}

// processBatches reads byte buffers from batchChan and writes them to the target server, while tracking stats on the write.
func processBatches(postgresConnect string) {
	db := sqlx.MustConnect("postgres", getConnectString())
	defer db.Close()

	for hypertableBatch := range batchChan {
		if !doLoad {
			continue
		}

		start := time.Now()
		columnCountWorker := processCSI(db, hypertableBatch)
		//columnCountWorker := processSplit(db, hypertableBatch)
		atomic.AddInt64(&columnCount, columnCountWorker)
		atomic.AddInt64(&rowCount, int64(len(hypertableBatch.rows)))

		if logBatches {
			now := time.Now()
			took := now.Sub(start)
			fmt.Printf("BATCH: time %d batchsize %d row rate %f/sec\n", now.Unix(), batchSize, float64(batchSize)/float64(took.Seconds()))
		}

	}
	workersGroup.Done()
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

func initBenchmarkDB(postgresConnect string, scanner *bufio.Scanner) {
	db := sqlx.MustConnect("postgres", postgresConnect)
	defer db.Close()
	db.MustExec("DROP DATABASE IF EXISTS " + databaseName)
	db.MustExec("CREATE DATABASE " + databaseName)

	dbBench := sqlx.MustConnect("postgres", getConnectString())
	defer dbBench.Close()

	if useHypertable {
		dbBench.MustExec("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE")
	}

	for scanner.Scan() {
		if len(scanner.Bytes()) == 0 {
			return
		}

		parts := strings.Split(scanner.Text(), ",")
		if parts[0] == "tags" {
			createTagsTable(dbBench, parts[1:])
			tableCols["tags"] = parts[1:]
			continue
		}

		hypertable := parts[0]
		partitioningField := tableCols["tags"][0]
		fieldDef := []string{}
		indexes := []string{}
		tableCols[hypertable] = parts[1:]

		psuedoCols := []string{partitioningField}
		psuedoCols = append(psuedoCols, parts[1:]...)
		for idx, field := range psuedoCols {
			if len(field) == 0 {
				continue
			}
			fieldType := "DOUBLE PRECISION"
			idxType := fieldIndex
			if idx == 0 {
				fieldType = "TEXT"
				idxType = ""
			}

			fieldDef = append(fieldDef, fmt.Sprintf("%s %s", field, fieldType))
			if fieldIndexCount == -1 || idx <= fieldIndexCount {
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
		//dbBench.MustExec(fmt.Sprintf("CREATE INDEX ON %s(tags_id, \"time\" DESC)", hypertable))
		//dbBench.MustExec(fmt.Sprintf("CREATE INDEX ON %s(\"time\" DESC, tags_id)", hypertable))

		for _, idxDef := range indexes {
			dbBench.MustExec(idxDef)
		}

		if useHypertable {
			dbBench.MustExec(
				fmt.Sprintf("SELECT create_hypertable('%s'::regclass, 'time'::name, partitioning_column => '%s'::name, number_partitions => %v::smallint, chunk_time_interval => %d)",
					hypertable, "tags_id", numberPartitions, chunkTime.Nanoseconds()/1000))
		} else {
			dbBench.MustExec(fmt.Sprintf("CREATE INDEX ON %s(tags_id, \"time\" DESC)", hypertable))
			dbBench.MustExec(fmt.Sprintf("CREATE INDEX ON %s(\"time\" DESC)", hypertable))
		}
	}
}
