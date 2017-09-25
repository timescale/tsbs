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
	"flag"
	"fmt"
	"log"
	"os"
	"runtime/pprof"
	"sync"
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/benchmarker"
	"bitbucket.org/440-labs/influxdb-comparisons/query"
)

const (
	BucketDuration   = 24 * time.Hour
	BucketTimeLayout = "2006-01-02"
	BlessedKeyspace  = "measurements"
)

// Blessed tables that hold benchmark data:
var (
	BlessedTables = []string{
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
	memProfile           string
)

// Helpers for choice-like flags:
var (
	aggrPlanChoices = map[string]int{
		"server": AggrPlanTypeWithServerAggregation,
		"client": AggrPlanTypeWithoutServerAggregation,
	}
)

// Global vars:
var (
	queryPool           = &query.CassandraPool
	queryChan           chan query.Query
	workersGroup        sync.WaitGroup
	aggrPlan            int
	benchmarkComponents *benchmarker.BenchmarkComponents
)

// Parse args:
func init() {
	benchmarkComponents = benchmarker.NewBenchmarkComponents()

	flag.StringVar(&daemonUrl, "url", "localhost:9042", "Cassandra URL.")
	flag.IntVar(&workers, "workers", 1, "Number of concurrent requests to make.")
	flag.StringVar(&aggrPlanLabel, "aggregation-plan", "", "Aggregation plan (choices: server, client)")
	flag.IntVar(&subQueryParallelism, "subquery-workers", 1, "Number of concurrent subqueries to make (because the client does a scatter+gather operation).")
	flag.DurationVar(&requestTimeout, "request-timeout", 1*time.Second, "Maximum request timeout.")
	flag.DurationVar(&csiTimeout, "client-side-index-timeout", 10*time.Second, "Maximum client-side index timeout (only used at initialization).")
	flag.IntVar(&debug, "debug", 0, "Whether to print debug messages.")
	flag.BoolVar(&prettyPrintResponses, "print-responses", false, "Pretty print response bodies (for correctness checking) (default false).")
	flag.StringVar(&memProfile, "memprofile", "", "Write a memory profile to this file.")

	flag.Parse()

	if _, ok := aggrPlanChoices[aggrPlanLabel]; !ok {
		log.Fatal("invalid aggregation plan")
	}
	aggrPlan = aggrPlanChoices[aggrPlanLabel]

}

func main() {
	// Make client-side index:
	csi := NewClientSideIndex(FetchSeriesCollection(daemonUrl, csiTimeout))

	// Make database connection pool:
	session := NewCassandraSession(daemonUrl, requestTimeout)
	defer session.Close()

	// Make data and stat channels:
	queryChan = make(chan query.Query, workers)

	// Launch the stats processor:
	go benchmarkComponents.StatProcessor.Process(workers)

	// Launch the query processors:
	qe := NewHLQueryExecutor(session, csi, debug)
	for i := 0; i < workers; i++ {
		workersGroup.Add(1)
		go processQueries(qe)
	}

	// Read in jobs, closing the job channel when done:
	input := bufio.NewReaderSize(os.Stdin, 1<<20)
	wallStart := time.Now()
	benchmarkComponents.Scanner.SetReader(input).Scan(queryPool, queryChan)
	close(queryChan)

	// Block for workers to finish sending requests, closing the stats
	// channel when done:
	workersGroup.Wait()
	benchmarkComponents.StatProcessor.CloseAndWait()

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

// processQueries reads byte buffers from queryChan and writes them to the
// target server, while tracking latency.
func processQueries(qc *HLQueryExecutor) {
	opts := HLQueryExecutorDoOptions{
		AggregationPlan:      aggrPlan,
		Debug:                debug,
		PrettyPrintResponses: prettyPrintResponses,
	}
	sp := benchmarkComponents.StatProcessor

	qFn := func(q *HLQuery, labels [][]byte, warm bool) {
		qpLagMs, reqLagMs, err := qc.Do(q, opts)
		if err != nil {
			log.Fatalf("Error during request: %s\n", err.Error())
		}

		// total stat
		totalMs := qpLagMs + reqLagMs
		if warm {
			sp.SendStat(append(labels[0], " (warm)"...), totalMs, true)
		} else {
			sp.SendStat(labels[0], totalMs, false)
		}

		// qp lag stat:
		if warm {
			sp.SendPartialStat(append(labels[1], " (warm)"...), qpLagMs, true)
		} else {
			sp.SendPartialStat(labels[1], qpLagMs, false)
		}

		// req lag stat:
		if warm {
			sp.SendPartialStat(append(labels[2], " (warm)"...), reqLagMs, true)
		} else {
			sp.SendPartialStat(labels[2], reqLagMs, false)
		}
	}

	labels := map[string][][]byte{}
	for q := range queryChan {
		cq := q.(*query.Cassandra)
		hlq := &HLQuery{*cq}
		hlq.ForceUTC()
		// if needed, prepare stat labels:
		if _, ok := labels[string(hlq.HumanLabel)]; !ok {
			labels[string(hlq.HumanLabel)] = [][]byte{
				hlq.HumanLabel,
				[]byte(fmt.Sprintf("%s-qp", hlq.HumanLabel)),
				[]byte(fmt.Sprintf("%s-req", hlq.HumanLabel)),
			}
		}
		ls := labels[string(hlq.HumanLabel)]

		qFn(hlq, ls, false) // cold run
		qFn(hlq, ls, true)  // warm run

		queryPool.Put(q)
	}
	workersGroup.Done()
}
