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

	"bitbucket.org/440-labs/influxdb-comparisons/query"
)

// Program option vars:
var (
	daemonUrls   []string
	databaseName string
	chunkSize    uint64
)

// Global vars:
var (
	queryPool       = &query.HTTPPool
	queryChan       chan query.Query
	benchmarkRunner *query.BenchmarkRunner
)

// Parse args:
func init() {
	benchmarkRunner = query.NewBenchmarkRunner()
	var csvDaemonUrls string

	flag.StringVar(&csvDaemonUrls, "urls", "http://localhost:8086", "Daemon URLs, comma-separated. Will be used in a round-robin fashion.")
	flag.StringVar(&databaseName, "db-name", "benchmark", "Name of database to use for queries")
	flag.Uint64Var(&chunkSize, "chunk-response-size", 0, "Number of series to chunk results into. 0 means no chunking.")

	flag.Parse()

	daemonUrls = strings.Split(csvDaemonUrls, ",")
	if len(daemonUrls) == 0 {
		log.Fatal("missing 'urls' flag")
	}
}

func main() {
	queryChan = make(chan query.Query, benchmarkRunner.Workers)
	benchmarkRunner.Run(queryPool, queryChan, processQueries)
}

// processQueries reads byte buffers from queryChan and writes them to the
// target server, while tracking latency.
func processQueries(wg *sync.WaitGroup, workerID int) {
	opts := &HTTPClientDoOptions{
		Debug:                benchmarkRunner.DebugLevel(),
		PrettyPrintResponses: benchmarkRunner.DoPrintResponses(),
		chunkSize:            chunkSize,
		database:             databaseName,
	}
	url := daemonUrls[workerID%len(daemonUrls)]
	w := NewHTTPClient(url)

	sp := benchmarkRunner.StatProcessor
	for q := range queryChan {
		lagMillis, err := w.Do(q.(*query.HTTP), opts)
		if err != nil {
			log.Fatalf("Error during request: %s\n", err.Error())
		}
		sp.SendStat(q.HumanLabelName(), lagMillis, !sp.PrewarmQueries)

		// If PrewarmQueries is set, we run the query as 'cold' first (see above),
		// then we immediately run it a second time and report that as the 'warm'
		// stat. This guarantees that the warm stat will reflect optimal cache performance.
		if sp.PrewarmQueries {
			lagMillis, err = w.Do(q.(*query.HTTP), opts)
			if err != nil {
				log.Fatalf("Error during request: %s\n", err.Error())
			}
			sp.SendStat(q.HumanLabelName(), lagMillis, true)
		}

		queryPool.Put(q)
	}
	wg.Done()
}
