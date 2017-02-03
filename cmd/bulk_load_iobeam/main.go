// bulk_load_cassandra loads a Cassandra daemon with data from stdin.
//
// The caller is responsible for assuring that the database is empty before
// bulk load.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"bitbucket.org/440-labs/postgres-kafka-consumer/meta"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

// Program option vars:
var (
	scriptsDir      string
	postgresConnect string
	workers         int
	batchSize       int
	doLoad          bool
	makeHypertable  bool
	tagIndex        string
	fieldIndex      string
	chunkSize       int

	//telemetry(atomic)
	columnCount int64
	rowCount    int64
)

var useUnlog = false
var useTemp = true

type hypertableBatch struct {
	hypertable string
	rows       []string
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
	//flag.StringVar(&scriptsDir, "scriptsDir", "/Users/arye/Development/go/src/bitbucket.org/440-labs/postgres-kafka-consumer/sql/scripts/", "Postgres connection url")

	flag.IntVar(&batchSize, "batch-size", 100, "Batch size (input items).")
	flag.IntVar(&workers, "workers", 1, "Number of parallel requests to make.")

	flag.BoolVar(&doLoad, "do-load", true, "Whether to write data. Set this flag to false to check input read speed.")
	flag.BoolVar(&makeHypertable, "make-hypertable", true, "Whether to make the table a hypertable. Set this flag to false to check input write speed and how much the insert logic slows things down.")

	flag.StringVar(&tagIndex, "tag-index", "VALUE-TIME,TIME-VALUE", "index types for tags (comma deliminated)")
	flag.StringVar(&fieldIndex, "field-index", "TIME-VALUE", "index types for tags (comma deliminated)")
	flag.IntVar(&chunkSize, "chunk-size", 1073741824, "Chunk size bytes")

	flag.Parse()

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

	if !makeHypertable {
		useTemp = false
		useUnlog = false
	}

	batchChan = make(chan *hypertableBatch, workers)
	inputDone = make(chan struct{})

	for i := 0; i < workers; i++ {
		workersGroup.Add(1)
		go processBatches(postgresConnect)
	}

	go report()

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

func report() {
	c := time.Tick(20 * time.Second)
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

		fmt.Printf("REPORT: col rate %f/sec row rate %f/sec (period) %f/sec (total) total rows %E\n", colrate, rowrate, overallRowrate, float64(rowCount))

		prevColCount = colCount
		prevRowCount = rowCount
		prevTime = now
	}

}

// scan reads lines from stdin. It expects input in the Iobeam format.
func scan(itemsPerBatch int, scanner *bufio.Scanner) int64 {
	batch := make(map[string][]string) // hypertable => copy lines
	var n int
	var linesRead int64
	for scanner.Scan() {
		linesRead++

		parts := strings.SplitN(scanner.Text(), ",", 2) //hypertable, copy line
		hypertable := parts[0]

		batch[hypertable] = append(batch[hypertable], parts[1])

		n++
		if n >= itemsPerBatch {
			for hypertable, rows := range batch {
				batchChan <- &hypertableBatch{hypertable, rows}
			}

			batch = make(map[string][]string)
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

	itemsRead := linesRead

	return itemsRead
}

// processBatches reads byte buffers from batchChan and writes them to the target server, while tracking stats on the write.
func processBatches(postgresConnect string) {
	dbBench := sqlx.MustConnect("postgres", postgresConnect+" dbname=benchmark")
	defer dbBench.Close()

	columnCountWorker := int64(0)
	for hypertableBatch := range batchChan {
		if !doLoad {
			continue
		}

		hypertable := hypertableBatch.hypertable

		tx := dbBench.MustBegin()
		copy_cmd := fmt.Sprintf("COPY \"%s\" FROM STDIN", hypertable)

		if useTemp {
			tx.MustExec(fmt.Sprintf("CREATE TEMP TABLE IF NOT EXISTS \"%s_temp\"( LIKE %s)", hypertable, hypertable))
			copy_cmd = fmt.Sprintf("COPY \"%s_temp\" FROM STDIN", hypertable)
		}

		if useUnlog {
			copy_cmd = fmt.Sprintf("COPY \"%s_unlog\" FROM STDIN", hypertable)
		}

		stmt, err := tx.Prepare(copy_cmd)
		if err != nil {
			panic(err)
		}
		for _, line := range hypertableBatch.rows {
			sp := strings.Split(line, ",")
			in := make([]interface{}, len(sp))
			columnCountWorker += int64(len(sp))
			for ind, value := range sp {
				in[ind] = value
			}
			_, err := stmt.Exec(in...)
			if err != nil {
				panic(err)
			}
		}
		atomic.AddInt64(&columnCount, columnCountWorker)
		atomic.AddInt64(&rowCount, int64(len(hypertableBatch.rows)))
		columnCountWorker = 0
		/*_, err = stmt.Exec()
		if err != nil {
			panic(err)
		}*/

		err = stmt.Close()
		if err != nil {
			panic(err)
		}

		if useTemp {
			tx.MustExec(fmt.Sprintf(`            
			SELECT _iobeamdb_internal.insert_data(
                (SELECT id FROM _iobeamdb_catalog.hypertable h
                WHERE h.schema_name = 'public' AND h.table_name = '%s')
				, '%s_temp'::regclass)`, hypertable, hypertable))
			tx.MustExec(fmt.Sprintf("TRUNCATE TABLE \"%s_temp\"", hypertable))

		}
		err = tx.Commit()
		if err != nil {
			panic(err)
		}

	}
	workersGroup.Done()
}

func initBenchmarkDB(postgresConnect string, scanner *bufio.Scanner) {
	db := sqlx.MustConnect("postgres", postgresConnect)
	defer db.Close()
	db.MustExec("DROP DATABASE IF EXISTS benchmark")
	db.MustExec("CREATE DATABASE benchmark")

	connect1 := meta.NewRemoteInfo(postgresConnect + " dbname=benchmark name=benchmark1")

	dbBench := sqlx.MustConnect("postgres", connect1.ConnectionString())
	defer dbBench.Close()

	dbBench.MustExec("CREATE EXTENSION IF NOT EXISTS iobeamdb CASCADE")
	dbBench.MustExec("SELECT setup_meta()")
	dbBench.MustExec("SELECT setup_main()")
	dbBench.MustExec("SELECT add_cluster_user('postgres', NULL)")
	dbBench.MustExec("SELECT set_meta('benchmark' :: NAME, 'fakehost')")
	dbBench.MustExec("SELECT add_node('benchmark' :: NAME, 'fakehost')")

	for scanner.Scan() {
		if len(scanner.Bytes()) == 0 {
			return
		}

		parts := strings.Split(scanner.Text(), ",")

		hypertable := parts[0]
		partitioning_field := ""
		field_def := []string{}
		indexes := []string{}

		for idx, field := range parts[1:] {
			if len(field) == 0 {
				continue
			}
			fieldType := "DOUBLE PRECISION"
			idxType := fieldIndex
			if idx == 0 {
				partitioning_field = field
				fieldType = "TEXT"
				idxType = tagIndex
			}

			field_def = append(field_def, fmt.Sprintf("%s %s", field, fieldType))
			for _, idx := range strings.Split(idxType, ",") {
				index_def := ""
				if idx == "TIME-VALUE" {
					index_def = fmt.Sprintf("(time, %s)", field)
				} else if idx == "VALUE-TIME" {
					index_def = fmt.Sprintf("(%s,time)", field)
				} else if idx != "" {
					panic(fmt.Sprintf("Unknown index type %v", idx))
				}

				if idx != "" {
					indexes = append(indexes, fmt.Sprintf("CREATE INDEX ON %s %s", hypertable, index_def))
					if useTemp {
						indexes = append(indexes, fmt.Sprintf("CREATE INDEX ON %s_temp_fake %s", hypertable, index_def))
					}
				}
			}
		}
		dbBench.MustExec(fmt.Sprintf("CREATE TABLE %s (time bigint, %s)", hypertable, strings.Join(field_def, ",")))

		if useUnlog {
			dbBench.MustExec(fmt.Sprintf("CREATE UNLOGGED TABLE %s_unlog (time bigint, %s)", hypertable, strings.Join(field_def, ",")))
			dbBench.MustExec(fmt.Sprintf(`
CREATE OR REPLACE FUNCTION _iobeamdb_internal.on_modify_%s()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
BEGIN
    EXECUTE format(
        $$
            SELECT _iobeamdb_internal.insert_data(
                (SELECT id FROM _iobeamdb_catalog.hypertable h
                WHERE h.schema_name = 'public' AND h.table_name = '%s')
                , %s)
        $$, TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_RELID);
    RETURN NEW;
END
$BODY$;
`, hypertable, hypertable, "%3$L"))
			dbBench.MustExec(fmt.Sprintf(`CREATE TRIGGER insert_trigger AFTER INSERT ON %s_unlog
                FOR EACH STATEMENT EXECUTE PROCEDURE _iobeamdb_internal.on_modify_%s();`, hypertable, hypertable))

		}

		if useTemp {
			dbBench.MustExec(fmt.Sprintf("CREATE TABLE %s_temp_fake (time bigint, %s)", hypertable, strings.Join(field_def, ",")))
		}

		for _, idx_def := range indexes {
			dbBench.MustExec(idx_def)
		}

		if makeHypertable {
			dbBench.MustExec(fmt.Sprintf("SELECT create_hypertable('%s', 'time', '%s', chunk_size_bytes => %v)", hypertable, partitioning_field, chunkSize))
		}

		dbBench.MustExec(fmt.Sprintf(`
			CREATE OR REPLACE FUNCTION io2ts(
				ts       bigint
			)
				RETURNS timestamp LANGUAGE SQL STABLE AS
			$BODY$
				SELECT to_timestamp(ts / 1000000000)::timestamp;
			$BODY$;
			`))
	}
}

func runScript(db *sqlx.DB, filename string) {
	content, err := ioutil.ReadFile(scriptsDir + filename)
	if err != nil {
		panic(err)
	}
	db.MustExec(string(content))
}
