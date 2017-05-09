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
	"sort"
	"sync"
	"time"

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
	limit                int64
	printInterval        int64
	memProfile           string
)

// Global vars:
var (
	queryPool    sync.Pool
	queryChan    chan *Query
	statPool     sync.Pool
	statChan     chan *Stat
	workersGroup sync.WaitGroup
	statGroup    sync.WaitGroup
)

// Parse args:
func init() {
	flag.StringVar(&postgresConnect, "postgres", "host=postgres user=postgres sslmode=disable", "Postgres connection url")
	flag.StringVar(&databaseName, "db-name", "benchmark", "Name of database to use for queries")
	flag.IntVar(&workers, "workers", 1, "Number of concurrent requests to make.")
	flag.IntVar(&debug, "debug", 0, "Whether to print debug messages.")
	flag.Int64Var(&limit, "limit", -1, "Limit the number of queries to send.")
	flag.Int64Var(&printInterval, "print-interval", 100, "Print timing stats to stderr after this many queries (0 to disable)")
	flag.BoolVar(&prettyPrintResponses, "print-responses", false, "Pretty print JSON response bodies (for correctness checking) (default false).")
	flag.StringVar(&memProfile, "memprofile", "", "Write a memory profile to this file.")

	flag.Parse()
}

func main() {
	// Make pools to minimize heap usage:
	queryPool = sync.Pool{
		New: func() interface{} {
			return &Query{
				HumanLabel:       make([]byte, 0, 1024),
				HumanDescription: make([]byte, 0, 1024),
				NamespaceName:    make([]byte, 0, 1024),
				FieldName:        make([]byte, 0, 1024),
				SqlQuery:         make([]byte, 0, 1024),
			}
		},
	}

	statPool = sync.Pool{
		New: func() interface{} {
			return &Stat{
				Label: make([]byte, 0, 1024),
				Value: 0.0,
			}
		},
	}
	// Make data and control channels:
	queryChan = make(chan *Query, workers)
	statChan = make(chan *Stat, workers)

	// Launch the stats processor:
	statGroup.Add(1)
	go processStats()

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
	close(statChan)

	// Wait on the stat collector to finish (and print its results):
	statGroup.Wait()

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

	n := int64(0)
	for {
		if limit >= 0 && n >= limit {
			break
		}

		q := queryPool.Get().(*Query)
		err := dec.Decode(q)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		q.ID = n

		queryChan <- q

		n++

	}
}

var mutex = &sync.Mutex{}

func prettyPrintJsonResponse(rows *sql.Rows, q *Query) {
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
	for q := range queryChan {

		query := string(q.SqlQuery)
		start := time.Now()

		rows, err := db.Queryx(query)
		if err != nil {
			panic(err)
		}

		if prettyPrintResponses {
			for rows.Next() {
				results := make(map[string]interface{})
				err = rows.MapScan(results)

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

		lag := float64(time.Since(start).Nanoseconds()) / 1e6 // milliseconds

		stat := statPool.Get().(*Stat)
		stat.Init(q.HumanLabel, lag)
		statChan <- stat

		queryPool.Put(q)

	}
	workersGroup.Done()
}

// processStats collects latency results, aggregating them into summary
// statistics. Optionally, they are printed to stderr at regular intervals.
func processStats() {
	const allQueriesLabel = "all queries"
	statMapping := map[string]*StatGroup{
		allQueriesLabel: &StatGroup{},
	}

	i := int64(0)
	for stat := range statChan {
		if _, ok := statMapping[string(stat.Label)]; !ok {
			statMapping[string(stat.Label)] = &StatGroup{}
		}

		statMapping[allQueriesLabel].Push(stat.Value)
		statMapping[string(stat.Label)].Push(stat.Value)

		statPool.Put(stat)

		i++

		// print stats to stderr (if printInterval is greater than zero):
		if printInterval > 0 && i > 0 && i%printInterval == 0 && (i < limit || limit < 0) {
			_, err := fmt.Fprintf(os.Stderr, "after %d queries with %d workers:\n", i, workers)
			if err != nil {
				log.Fatal(err)
			}
			fprintStats(os.Stderr, statMapping)
			_, err = fmt.Fprintf(os.Stderr, "\n")
			if err != nil {
				log.Fatal(err)
			}
		}
	}

	// the final stats output goes to stdout:
	_, err := fmt.Printf("run complete after %d queries with %d workers:\n", i, workers)
	if err != nil {
		log.Fatal(err)
	}
	fprintStats(os.Stdout, statMapping)
	statGroup.Done()
}

// fprintStats pretty-prints stats to the given writer.
func fprintStats(w io.Writer, statGroups map[string]*StatGroup) {
	maxKeyLength := 0
	keys := make([]string, 0, len(statGroups))
	for k := range statGroups {
		if len(k) > maxKeyLength {
			maxKeyLength = len(k)
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		v := statGroups[k]
		minRate := 1e3 / v.Min
		meanRate := 1e3 / v.Mean
		maxRate := 1e3 / v.Max
		paddedKey := fmt.Sprintf("%s", k)
		for len(paddedKey) < maxKeyLength {
			paddedKey += " "
		}
		_, err := fmt.Fprintf(w, "%s:\n min: %8.2fms (%7.2f/sec), mean: %8.2fms (%7.2f/sec), max: %7.2fms (%6.2f/sec), stddev: %8.2f, sum: %5.1fsec \n", paddedKey, v.Min, minRate, v.Mean, meanRate, v.Max, maxRate, v.StdDev, v.Sum/1e3)
		if err != nil {
			log.Fatal(err)
		}
	}

}
