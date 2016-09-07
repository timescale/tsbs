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
	"github.com/lib/pq"
)

// Program option vars:
var (
	daemonUrl       string
	scriptsDir      string
	postgresConnect string
	workers         int
	batchSize       int
	doLoad          bool
	writeTimeout    time.Duration
)

const ProjectID = 1
const ReplicaNo = 1
const NumPartitions = 1
const Partition = 0

type row struct {
	namespace string
	time      string
	host      string
	uuid      string
	json      string
}

// Global vars
var (
	batchChan    chan []row
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

	batchChan = make(chan []row, workers)
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

// scan reads lines from stdin. It expects input in the Cassandra CQL format.
func scan(itemsPerBatch int, scanner *bufio.Scanner) int64 {
	var batch []row
	batch = make([]row, 0, itemsPerBatch)

	var n int
	var linesRead int64
	for scanner.Scan() {
		linesRead++

		parts := strings.SplitN(scanner.Text(), " ", 5)
		dataRow := row{parts[0], parts[1], parts[2], parts[3], parts[4]}

		batch = append(batch, dataRow)

		n++
		if n >= itemsPerBatch {
			batchChan <- batch
			batch = make([]row, 0, itemsPerBatch)
			n = 0
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("Error reading input: %s", err.Error())
	}

	// Finished reading input, make sure last batch goes out.
	if n > 0 {
		batchChan <- batch
	}

	// Closing inputDone signals to the application that we've read everything and can now shut down.
	close(inputDone)

	// The Cassandra query format uses 1 line per item:
	itemsRead := linesRead

	return itemsRead
}

// processBatches reads byte buffers from batchChan and writes them to the target server, while tracking stats on the write.
func processBatches(postgresConnect string) {
	connect1 := meta.NewRemoteInfo(postgresConnect + " dbname=benchmark name=benchmark1")
	dbBench := sqlx.MustConnect("postgres", connect1.ConnectionString())
	defer dbBench.Close()

	for batch := range batchChan {
		if !doLoad {
			continue
		}

		table_name := "copy_t"

		tx := dbBench.MustBegin()
		tx.MustExec("SELECT 1 FROM  create_temp_copy_table_one_partition($1, $2, $3, $4, $5)", table_name, ProjectID, ReplicaNo, Partition, NumPartitions)
		stmt, err := tx.Prepare(pq.CopyIn(table_name, "namespace", "time", "partition_key", "value", "uuid", "row_id"))
		if err != nil {
			panic(err)
		}

		for idx, row := range batch {
			_, err := stmt.Exec(row.namespace, row.time, row.host, row.json, batch[0].uuid, idx)
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
		tx.MustExec("SELECT 1 FROM  insert_data_one_partition($1, $2, $3, $4, $5)", table_name, ProjectID, ReplicaNo, Partition, NumPartitions)
		tx.Commit()
		/*	// Write the batch.
			err := session.ExecuteBatch(batch)
			if err != nil {
				log.Fatalf("Error writing: %s\n", err.Error())
			}*/
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

	runScript(dbBench, "extensions.sql")
	runScript(dbBench, "tables.sql")

	runScript(dbBench, "query.sql")
	runScript(dbBench, "partitioning.sql")
	runScript(dbBench, "insert.sql")
	runScript(dbBench, "util.sql")
	runScript(dbBench, "unoptimized.sql")

	runScript(dbBench, "ioql.sql")
	runScript(dbBench, "ioql_unoptimized.sql")
	runScript(dbBench, "ioql_optimized.sql")
	runScript(dbBench, "ioql_optimized_agg.sql")
	runScript(dbBench, "ioql_optimized_nonagg.sql")

	dbBench.MustExec(`	
	CREATE TABLE IF NOT EXISTS cluster.project_field (
		project_id BIGINT,
		namespace TEXT,
		field TEXT,
		cluster_table regclass,
		value_type regtype,
		is_partition_key boolean,
		is_distinct boolean,
		server_name TEXT,
		PRIMARY KEY (project_id, namespace, field)
);

CREATE OR REPLACE FUNCTION register_project_field(
		project_id BIGINT,
		namespace TEXT,
		field TEXT,
		cluster_table regclass,
		value_type regtype,
		is_partition_key boolean,
		is_distinct boolean
) RETURNS VOID AS $$
	INSERT INTO cluster.project_field (SELECT project_id, namespace, field, cluster_table, value_type, is_partition_key, is_distinct, name FROM public.cluster_name) 
  ON CONFLICT DO NOTHING
$$
LANGUAGE 'sql' VOLATILE;
`)

	dbBench.MustExec(fmt.Sprintf("DROP SERVER IF EXISTS \"local\" CASCADE"))
	dbBench.MustExec(fmt.Sprintf("CREATE SERVER \"local\" FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host 'localhost', dbname 'benchmark')"))
	dbBench.MustExec(fmt.Sprintf("CREATE USER MAPPING FOR CURRENT_USER SERVER \"local\" OPTIONS (user 'postgres')"))

	dbBench.MustExec(`CREATE OR REPLACE FUNCTION register_global_table(
	local_table_name text, 
	cluster_table_name text
) RETURNS VOID AS $$
	$$
LANGUAGE 'sql' VOLATILE`)

	for scanner.Scan() {
		if len(scanner.Bytes()) == 0 {
			return
		}

		parts := strings.Split(scanner.Text(), ",")

		namespace := parts[0]
		dbBench.MustExec("SELECT 1 FROM create_cluster_table($1, $2, $3, $4, $5)", ProjectID, namespace, ReplicaNo, Partition, NumPartitions)
		dbBench.MustExec("SELECT 1 FROM create_master_table($1, $2, $3, $4, $5)", ProjectID, namespace, ReplicaNo, Partition, NumPartitions)
		dbBench.MustExec("SELECT 1 FROM create_partition_table($1, $2, $3, $4, $5)", ProjectID, namespace, ReplicaNo, Partition, NumPartitions)
		for idx, field := range parts[1:] {
			if len(field) == 0 {
				continue
			}
			isDistinct := idx == 0
			isPartitioning := idx == 0
			fieldType := "DOUBLE PRECISION"
			if idx == 0 {
				fieldType = "TEXT"
			}
			dbBench.MustExec("SELECT 1 FROM register_project_field($1, $2, $3,$4::regclass, $5::regtype, $6, $7)", ProjectID, namespace, field, fmt.Sprintf("cluster.%d_%s_%d", ProjectID, namespace, ReplicaNo), fieldType, isPartitioning, isDistinct)
		}

	}
}

func runScript(db *sqlx.DB, filename string) {
	content, err := ioutil.ReadFile(scriptsDir + filename)
	if err != nil {
		panic(err)
	}
	db.MustExec(string(content))
}
