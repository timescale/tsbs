// query_benchmarker speed tests TimescaleDB using requests from stdin.
//
// It reads encoded Query objects from stdin, and makes concurrent requests
// to the provided PostgreSQL/TimescaleDB endpoint. This program has no knowledge of the
// internals of the endpoint.
package main

import (
	"bufio"
	"encoding/gob"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime/pprof"
	"sync"
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/benchmarker"
	"bitbucket.org/440-labs/influxdb-comparisons/query"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

// Program option vars:
var (
	postgresConnect      string
	databaseName         string
	workers              int
	debug                int
	prettyPrintResponses bool
	showExplain          bool
	memProfile           string
)

// Global vars:
var (
	queryPool     = &query.TimescaleDBPool
	queryChan     chan *query.TimescaleDB
	workersGroup  sync.WaitGroup
	statProcessor *benchmarker.StatProcessor
)

// Parse args:
func init() {
	statProcessor = benchmarker.NewStatProcessor()

	flag.StringVar(&postgresConnect, "postgres", "host=postgres user=postgres sslmode=disable", "Postgres connection url")
	flag.StringVar(&databaseName, "db-name", "benchmark", "Name of database to use for queries")
	flag.IntVar(&workers, "workers", 1, "Number of concurrent requests to make.")
	flag.IntVar(&debug, "debug", 0, "Whether to print debug messages.")
	flag.BoolVar(&prettyPrintResponses, "print-responses", false, "Pretty print JSON response bodies (for correctness checking) (default false).")
	flag.BoolVar(&showExplain, "show-explain", false, "Print out the EXPLAIN output for sample query")
	flag.StringVar(&memProfile, "memprofile", "", "Write a memory profile to this file.")

	flag.Parse()

	if showExplain {
		statProcessor.Limit = 1
	}
}

func main() {
	// Make data and control channels:
	queryChan = make(chan *query.TimescaleDB, workers)

	// Launch the stats processor:
	go statProcessor.Process(workers)

	// Launch the query processors:
	for i := 0; i < workers; i++ {
		workersGroup.Add(1)
		go processQueries()
	}

	// Read in jobs, closing the job channel when done:
	input := bufio.NewReaderSize(os.Stdin, 1<<20)
	wallStart := time.Now()
	scan(input)
	close(queryChan)

	// Block for workers to finish sending requests, closing the stats
	// channel when done:
	workersGroup.Wait()
	close(statProcessor.C)

	// Wait on the stat collector to finish (and print its results):
	statProcessor.Wait()

	wallEnd := time.Now()
	wallTook := wallEnd.Sub(wallStart)
	_, err := fmt.Printf("wall clock time: %fsec\n", float64(wallTook.Nanoseconds())/1e9)
	if err != nil {
		log.Fatal(err)
	}

	// (Optional) create a memory profile:
	if memProfile != "" {
		f, err := os.Create(memProfile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.WriteHeapProfile(f)
		f.Close()
	}
}

func getConnectString() string {
	return postgresConnect + " dbname=" + databaseName
}

// scan reads encoded Queries and places them onto the workqueue.
func scan(r io.Reader) {
	dec := gob.NewDecoder(r)

	n := uint64(0)
	for {
		if statProcessor.Limit >= 0 && n >= statProcessor.Limit {
			break
		}

		q := queryPool.Get().(*query.TimescaleDB)
		err := dec.Decode(q)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		q.ID = int64(n)

		queryChan <- q

		n++

	}
}

// prettyPrintResponse prints a Query and its response in JSON format with two
// keys: 'query' which has a value of the SQL used to generate the second key
// 'results' which is an array of each row in the return set.
func prettyPrintResponse(rows *sqlx.Rows, q *query.TimescaleDB) {
	resp := make(map[string]interface{})
	resp["query"] = string(q.SqlQuery)

	results := []map[string]interface{}{}
	for rows.Next() {
		r := make(map[string]interface{})
		if err := rows.MapScan(r); err != nil {
			panic(err)
		}
		results = append(results, r)
		resp["results"] = results
	}

	line, err := json.MarshalIndent(resp, "", "  ")
	if err != nil {
		panic(err)
	}

	fmt.Println(string(line) + "\n")
}

type queryExecutorOptions struct {
	showExplain   bool
	debug         bool
	printResponse bool
}

type queryExecutor struct {
	db *sqlx.DB
}

func newQueryExecutor(conn string) *queryExecutor {
	return &queryExecutor{
		db: sqlx.MustConnect("postgres", conn),
	}
}

func (qe *queryExecutor) Do(q query.Query, opts *queryExecutorOptions) (float64, error) {
	start := time.Now()
	qry := string(q.(*query.TimescaleDB).SqlQuery)
	if showExplain {
		qry = "EXPLAIN ANALYZE " + qry
	}
	rows, err := qe.db.Queryx(qry)
	took := float64(time.Since(start).Nanoseconds()) / 1e6
	if err != nil {
		return took, err
	}

	if debug > 0 {
		fmt.Println(qry)
	}
	if showExplain {
		text := ""
		for rows.Next() {
			var s string
			if err2 := rows.Scan(&s); err2 != nil {
				panic(err2)
			}
			text += s + "\n"
		}
		fmt.Printf("%s\n\n%s\n-----\n\n", qry, text)
	} else if prettyPrintResponses {
		prettyPrintResponse(rows, q.(*query.TimescaleDB))
	}
	rows.Close()
	return took, err
}

// processQueries reads byte buffers from queryChan and writes them to the
// target server, while tracking latency.
func processQueries() {
	qe := newQueryExecutor(getConnectString())
	opts := &queryExecutorOptions{
		showExplain:   showExplain,
		debug:         debug > 0,
		printResponse: prettyPrintResponses,
	}

	for q := range queryChan {
		lag, err := qe.Do(q, opts)
		if err != nil {
			panic(err)
		}
		stat := statProcessor.GetStat()
		stat.Init(q.HumanLabel, lag)
		statProcessor.C <- stat

		if !showExplain {
			// Warm run
			lag, err = qe.Do(q, &queryExecutorOptions{})
			if err != nil {
				panic(err)
			}
			stat = statProcessor.GetStat()
			stat.InitWarm(q.HumanLabel, lag)
			statProcessor.C <- stat
		}
		queryPool.Put(q)

	}
	workersGroup.Done()
}
