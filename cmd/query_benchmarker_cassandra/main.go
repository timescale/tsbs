// query_benchmarker_cassandra speed tests Cassandra servers using request
// data from stdin.
//
// It reads encoded HLQuery objects from stdin, and makes concurrent requests
// to the provided Cassandra cluster. This program is a 'heavy client', i.e.
// it builds a client-side index of table metadata before beginning the
// benchmarking.
//
// TODO(rw): On my machine, this only decodes 700k/sec messages from stdin.
package main

import (
	"bufio"
	"encoding/gob"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime/pprof"
	"sync"
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/benchmarker"
)

const (
	BucketDuration   time.Duration = 24 * time.Hour
	BucketTimeLayout string        = "2006-01-02"
	BlessedKeyspace  string        = "measurements"
)

// Blessed tables that hold benchmark data:
var (
	BlessedTables []string = []string{
		"series_bigint",
		"series_float",
		"series_double",
		"series_boolean",
		"series_blob",
	}
)

// Program option vars:
var (
	daemonUrl            string
	workers              int
	aggrPlanLabel        string
	subQueryParallelism  int
	requestTimeout       time.Duration
	csiTimeout           time.Duration
	debug                int
	prettyPrintResponses bool
	limit                int64
	burnIn               uint64
	printInterval        uint64
	memProfile           string
)

// Helpers for choice-like flags:
var (
	aggrPlanChoices map[string]int = map[string]int{
		"server": AggrPlanTypeWithServerAggregation,
		"client": AggrPlanTypeWithoutServerAggregation,
	}
)

// Global vars:
var (
	queryPool    sync.Pool
	hlQueryChan  chan *HLQuery
	statPool     sync.Pool
	statChan     chan *CStat
	workersGroup sync.WaitGroup
	statGroup    sync.WaitGroup
	aggrPlan     int
)

// Parse args:
func init() {
	flag.StringVar(&daemonUrl, "url", "localhost:9042", "Cassandra URL.")
	flag.IntVar(&workers, "workers", 1, "Number of concurrent requests to make.")
	flag.StringVar(&aggrPlanLabel, "aggregation-plan", "", "Aggregation plan (choices: server, client)")
	flag.IntVar(&subQueryParallelism, "subquery-workers", 1, "Number of concurrent subqueries to make (because the client does a scatter+gather operation).")
	flag.DurationVar(&requestTimeout, "request-timeout", 1*time.Second, "Maximum request timeout.")
	flag.DurationVar(&csiTimeout, "client-side-index-timeout", 10*time.Second, "Maximum client-side index timeout (only used at initialization).")
	flag.IntVar(&debug, "debug", 0, "Whether to print debug messages.")
	flag.Int64Var(&limit, "limit", -1, "Limit the number of queries to send.")
	flag.Uint64Var(&burnIn, "burn-in", 0, "Number of queries to ignore before collecting statistics.")
	flag.Uint64Var(&printInterval, "print-interval", 100, "Print timing stats to stderr after this many queries (0 to disable)")
	flag.BoolVar(&prettyPrintResponses, "print-responses", false, "Pretty print response bodies (for correctness checking) (default false).")
	flag.StringVar(&memProfile, "memprofile", "", "Write a memory profile to this file.")

	flag.Parse()

	if _, ok := aggrPlanChoices[aggrPlanLabel]; !ok {
		log.Fatal("invalid aggregation plan")
	}
	aggrPlan = aggrPlanChoices[aggrPlanLabel]
}

func main() {
	// Make pools to minimize heap usage:
	queryPool = sync.Pool{
		New: func() interface{} {
			return &HLQuery{
				HumanLabel:       make([]byte, 0, 1024),
				HumanDescription: make([]byte, 0, 1024),
				MeasurementName:  make([]byte, 0, 1024),
				FieldName:        make([]byte, 0, 1024),
				AggregationType:  make([]byte, 0, 1024),
				WhereClause:      make([]byte, 0, 1024),
				OrderBy:          make([]byte, 0, 1024),
				TagSets:          make([][]string, 0, 10),
			}
		},
	}

	statPool = sync.Pool{
		New: func() interface{} {
			return &CStat{
				Stat: benchmarker.Stat{
					Label: make([]byte, 0, 1024),
				},
			}
		},
	}

	// Make client-side index:
	csi := NewClientSideIndex(FetchSeriesCollection(daemonUrl, csiTimeout))

	// Make database connection pool:
	session := NewCassandraSession(daemonUrl, requestTimeout)
	defer session.Close()

	// Make data and stat channels:
	hlQueryChan = make(chan *HLQuery, workers)
	statChan = make(chan *CStat, workers)

	// Launch the stats processor:
	statGroup.Add(1)
	go processStats()

	// Launch the query processors:
	qe := NewHLQueryExecutor(session, csi, debug)
	for i := 0; i < workers; i++ {
		workersGroup.Add(1)
		go processQueries(qe)
	}

	// Read in jobs, closing the job channel when done:
	input := bufio.NewReaderSize(os.Stdin, 1<<20)
	wallStart := time.Now()
	scan(input)
	close(hlQueryChan)

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

// scan reads encoded Queries and places them onto the workqueue.
func scan(r io.Reader) {
	dec := gob.NewDecoder(r)

	n := int64(0)
	for {
		if limit >= 0 && n >= limit {
			break
		}

		q := queryPool.Get().(*HLQuery)
		err := dec.Decode(q)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		q.ID = n
		q.ForceUTC()

		hlQueryChan <- q

		n++
	}
}

// processQueries reads byte buffers from hlQueryChan and writes them to the
// target server, while tracking latency.
func processQueries(qc *HLQueryExecutor) {
	opts := HLQueryExecutorDoOptions{
		AggregationPlan:      aggrPlan,
		Debug:                debug,
		PrettyPrintResponses: prettyPrintResponses,
	}
	labels := map[string][][]byte{}
	for q := range hlQueryChan {
		qpLagMs, reqLagMs, err := qc.Do(q, opts)

		// if needed, prepare stat labels:
		if _, ok := labels[string(q.HumanLabel)]; !ok {
			labels[string(q.HumanLabel)] = [][]byte{
				q.HumanLabel,
				[]byte(fmt.Sprintf("%s-qp", q.HumanLabel)),
				[]byte(fmt.Sprintf("%s-req", q.HumanLabel)),
			}
		}
		ls := labels[string(q.HumanLabel)]

		// total lag stat:
		stat := statPool.Get().(*CStat)
		stat.Init(ls[0], qpLagMs+reqLagMs, true)
		statChan <- stat

		// qp lag stat:
		stat = statPool.Get().(*CStat)
		stat.Init(ls[1], qpLagMs, false)
		statChan <- stat

		// req lag stat:
		stat = statPool.Get().(*CStat)
		stat.Init(ls[2], reqLagMs, false)
		statChan <- stat

		queryPool.Put(q)
		if err != nil {
			log.Fatalf("Error during request: %s\n", err.Error())
		}
	}
	workersGroup.Done()
}

// processStats collects latency results, aggregating them into summary
// statistics. Optionally, they are printed to stderr at regular intervals.
func processStats() {
	statMapping := map[string]*benchmarker.StatGroup{}

	i := uint64(0)
	for stat := range statChan {
		if i < burnIn {
			i++
			statPool.Put(stat)
			continue
		} else if i == burnIn && burnIn > 0 {
			_, err := fmt.Fprintf(os.Stderr, "burn-in complete after %d queries with %d workers\n", burnIn, workers)
			if err != nil {
				log.Fatal(err)
			}
		}
		if _, ok := statMapping[string(stat.Label)]; !ok {
			statMapping[string(stat.Label)] = &benchmarker.StatGroup{}
		}

		statMapping[string(stat.Label)].Push(stat.Value)

		if stat.IsActual {
			i++
		}

		statPool.Put(stat)

		// print stats to stderr (if printInterval is greater than zero):
		if printInterval > 0 && i > 0 && i%printInterval == 0 && (int64(i) < limit || limit < 0) {
			_, err := fmt.Fprintf(os.Stderr, "after %d queries with %d workers:\n", i-burnIn, workers)
			if err != nil {
				log.Fatal(err)
			}
			benchmarker.WriteStatGroupMap(os.Stderr, statMapping)
			_, err = fmt.Fprintf(os.Stderr, "\n")
			if err != nil {
				log.Fatal(err)
			}
		}
	}

	// the final stats output goes to stdout:
	_, err := fmt.Printf("run complete after %d queries with %d workers:\n", i-burnIn, workers)
	if err != nil {
		log.Fatal(err)
	}
	benchmarker.WriteStatGroupMap(os.Stdout, statMapping)
	statGroup.Done()
}
