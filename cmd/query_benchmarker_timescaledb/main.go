// query_benchmarker speed tests TimescaleDB using requests from stdin.
//
// It reads encoded Query objects from stdin, and makes concurrent requests
// to the provided HTTP endpoint. This program has no knowledge of the
// internals of the endpoint.
//
// TODO(rw): On my machine, this only decodes 700k/sec messages from stdin.
package main

import (
	"bufio"
	"bytes"
	"database/sql"
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

var mutex = &sync.Mutex{}

func prettyPrintJsonResponse(rows *sql.Rows, q *query.TimescaleDB) {
	var result bytes.Buffer

	for rows.Next() {
		var jsonRow string
		err := rows.Scan(&jsonRow)

		if err != nil {
			panic(err)
		}

		var response = make(map[string]interface{})
		err = json.Unmarshal([]byte(jsonRow), &response)

		if err != nil {
			panic(err)
		}

		splitFactor := int64(1000000000)

		if _, ok := response["group_time"]; ok {
			ts := int64(response["group_time"].(float64))
			timeStruct := time.Unix(ts/splitFactor, ts%splitFactor)
			response["group_time"] = timeStruct.Format(time.RFC3339)
		}

		if _, ok := response["time"]; ok {
			ts := int64(response["time"].(float64))
			timeStruct := time.Unix(ts/splitFactor, ts%splitFactor)
			response["time"] = timeStruct.Format(time.RFC3339)
		}

		modifiedResponse, err := json.Marshal(response)
		if err != nil {
			panic(err)
		}
		var pretty bytes.Buffer
		prefix := fmt.Sprintf("ID %d: ", q.ID)

		err = json.Indent(&pretty, modifiedResponse, prefix, "  ")

		if err != nil {
			return
		}

		_, err = fmt.Fprintf(&result, "%s%s\n", prefix, pretty.Bytes())
		if err != nil {
			return
		}
	}

	mutex.Lock()
	defer mutex.Unlock()
	result.WriteTo(os.Stderr)

}

// processQueries reads byte buffers from queryChan and writes them to the
// target server, while tracking latency.
func processQueries() {
	db := sqlx.MustConnect("postgres", getConnectString())
	qFn := func(query string) (*sqlx.Rows, float64) {
		start := time.Now()
		rows, err := db.Queryx(query)
		if err != nil {
			panic(err)
		}
		return rows, float64(time.Since(start).Nanoseconds()) / 1e6
	}

	for q := range queryChan {
		query := string(q.SqlQuery)
		if showExplain {
			query = "EXPLAIN ANALYZE " + query
		}

		rows, lag := qFn(query)
		if showExplain {
			fmt.Printf(query + "\n\n")
			for rows.Next() {
				var s string
				if err := rows.Scan(&s); err != nil {
					panic(err)
				}
				fmt.Println(s)
			}
			fmt.Printf("\n-----\n\n")
		} else if prettyPrintResponses {
			for rows.Next() {
				results := make(map[string]interface{})
				err := rows.MapScan(results)

				if err != nil {
					panic(err)
				}

				line, err := json.MarshalIndent(results, "", "  ")
				if err != nil {
					panic(err)
				}

				fmt.Println(string(line))
			}
		}
		rows.Close()

		if debug > 0 {
			fmt.Println(query)
		}

		if showExplain {
			queryPool.Put(q)
			continue
		}

		stat := statProcessor.GetStat()
		stat.Init(q.HumanLabel, lag)
		statProcessor.C <- stat

		// Warm run
		rows, lag = qFn(query)
		stat = statProcessor.GetStat()
		stat.InitWarm(q.HumanLabel, lag)
		statProcessor.C <- stat
		rows.Close()

		queryPool.Put(q)

	}
	workersGroup.Done()
}
