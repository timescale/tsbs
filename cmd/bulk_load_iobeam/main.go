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
	tagIndex        string
	fieldIndex      string
)

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
	flag.StringVar(&scriptsDir, "scriptsDir", "/Users/arye/Development/go/src/bitbucket.org/440-labs/postgres-kafka-consumer/sql/scripts/", "Postgres connection url")

	flag.IntVar(&batchSize, "batch-size", 100, "Batch size (input items).")
	flag.IntVar(&workers, "workers", 1, "Number of parallel requests to make.")

	flag.BoolVar(&doLoad, "do-load", true, "Whether to write data. Set this flag to false to check input read speed.")

	flag.StringVar(&tagIndex, "tag-index", "VALUE-TIME,TIME-VALUE", "index types for tags (comma deliminated)")
	flag.StringVar(&fieldIndex, "field-index", "TIME-VALUE", "index types for tags (comma deliminated)")

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

	batchChan = make(chan *hypertableBatch, workers)
	inputDone = make(chan struct{})

	for i := 0; i < workers; i++ {
		workersGroup.Add(1)
		go processBatches(postgresConnect)
	}

	start := time.Now()
	itemsRead := scan(batchSize, scanner)

	<-inputDone
	close(batchChan)
	workersGroup.Wait()
	end := time.Now()
	took := end.Sub(start)
	rate := float64(itemsRead) / float64(took.Seconds())

	fmt.Printf("loaded %d items in %fsec with %d workers (mean rate %f/sec)\n", itemsRead, took.Seconds(), workers, rate)
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

	// The Cassandra query format uses 1 line per item:
	itemsRead := linesRead

	return itemsRead
}

// processBatches reads byte buffers from batchChan and writes them to the target server, while tracking stats on the write.
func processBatches(postgresConnect string) {
	dbBench := sqlx.MustConnect("postgres", postgresConnect+" dbname=benchmark")
	defer dbBench.Close()

	for hypertableBatch := range batchChan {
		if !doLoad {
			continue
		}

		hypertable := hypertableBatch.hypertable

		tx := dbBench.MustBegin()
		stmt, err := tx.Prepare(fmt.Sprintf("COPY \"%s\" FROM STDIN", hypertable))
		if err != nil {
			panic(err)
		}
		for _, line := range hypertableBatch.rows {
			sp := strings.Split(line, ",")
			in := make([]interface{}, len(sp))
			for ind, value := range sp {
				in[ind] = value
			}
			_, err := stmt.Exec(in...)
			if err != nil {
				panic(err)
			}
		}
		/*_, err = stmt.Exec()
		if err != nil {
			panic(err)
		}*/

		err = stmt.Close()
		if err != nil {
			panic(err)
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
					index_def = fmt.Sprintf("CREATE INDEX ON %s(time, %s)", hypertable, field)
				} else if idx == "VALUE-TIME" {
					index_def = fmt.Sprintf("CREATE INDEX ON %s(%s,time)", hypertable, field)
				} else if idx != "" {
					panic(fmt.Sprintf("Unknown index type %v", idx))
				}
				indexes = append(indexes, index_def)
			}
		}

		dbBench.MustExec(fmt.Sprintf("CREATE TABLE %s (time bigint, %s)", hypertable, strings.Join(field_def, ",")))
		for _, idx_def := range indexes {
			dbBench.MustExec(idx_def)
		}
		dbBench.MustExec(fmt.Sprintf("SELECT create_hypertable('%s', 'time', '%s')", hypertable, partitioning_field))
	}
}

func runScript(db *sqlx.DB, filename string) {
	content, err := ioutil.ReadFile(scriptsDir + filename)
	if err != nil {
		panic(err)
	}
	db.MustExec(string(content))
}
