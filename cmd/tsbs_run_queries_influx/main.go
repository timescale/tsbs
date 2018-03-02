// tsbs_run_queries_influx speed tests InfluxDB using requests from stdin.
//
// It reads encoded Query objects from stdin, and makes concurrent requests
// to the provided HTTP endpoint. This program has no knowledge of the
// internals of the endpoint.
package main

import (
	"flag"
	"log"
	"strings"
	"sync"

	"bitbucket.org/440-labs/influxdb-comparisons/benchmarker"
	"bitbucket.org/440-labs/influxdb-comparisons/query"
)

// Program option vars:
var (
	daemonUrls           []string
	databaseName         string
	debug                int
	prettyPrintResponses bool
	chunkSize            uint64
)

// Global vars:
var (
	queryPool           = &query.HTTPPool
	queryChan           chan query.Query
	benchmarkComponents *benchmarker.BenchmarkComponents
)

// Parse args:
func init() {
	benchmarkComponents = benchmarker.NewBenchmarkComponents()
	var csvDaemonUrls string

	flag.StringVar(&csvDaemonUrls, "urls", "http://localhost:8086", "Daemon URLs, comma-separated. Will be used in a round-robin fashion.")
	flag.StringVar(&databaseName, "db-name", "benchmark", "Name of database to use for queries")
	flag.IntVar(&debug, "debug", 0, "Whether to print debug messages.")
	flag.BoolVar(&prettyPrintResponses, "print-responses", false, "Pretty print JSON response bodies (for correctness checking) (default false).")
	flag.Uint64Var(&chunkSize, "chunk-response-size", 0, "Number of series to chunk results into. 0 means no chunking.")

	flag.Parse()

	daemonUrls = strings.Split(csvDaemonUrls, ",")
	if len(daemonUrls) == 0 {
		log.Fatal("missing 'urls' flag")
	}
}

func main() {
	queryChan = make(chan query.Query, benchmarkComponents.Workers)
	benchmarkComponents.Run(queryPool, queryChan, processQueries)
}

// processQueries reads byte buffers from queryChan and writes them to the
// target server, while tracking latency.
func processQueries(wg *sync.WaitGroup, workerID int) {
	opts := &HTTPClientDoOptions{
		Debug:                debug,
		PrettyPrintResponses: prettyPrintResponses,
		chunkSize:            chunkSize,
		database:             databaseName,
	}
	url := daemonUrls[workerID%len(daemonUrls)]
	w := NewHTTPClient(url)

	sp := benchmarkComponents.StatProcessor
	for q := range queryChan {
		lagMillis, err := w.Do(q.(*query.HTTP), opts)
		if err != nil {
			log.Fatalf("Error during request: %s\n", err.Error())
		}
		sp.SendStat(q.HumanLabelName(), lagMillis, false)

		// Warm run
		lagMillis, err = w.Do(q.(*query.HTTP), opts)
		if err != nil {
			log.Fatalf("Error during request: %s\n", err.Error())
		}
		sp.SendStat(q.HumanLabelName(), lagMillis, true)

		queryPool.Put(q)
	}
	wg.Done()
}
