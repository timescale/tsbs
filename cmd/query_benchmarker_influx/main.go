// query_benchmarker speed tests InfluxDB using requests from stdin.
//
// It reads encoded Query objects from stdin, and makes concurrent requests
// to the provided HTTP endpoint. This program has no knowledge of the
// internals of the endpoint.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime/pprof"
	"strings"
	"sync"
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/benchmarker"
	"bitbucket.org/440-labs/influxdb-comparisons/query"
)

// Program option vars:
var (
	daemonUrls           []string
	databaseName         string
	workers              int
	debug                int
	prettyPrintResponses bool
	chunkSize            uint64
	memProfile           string
)

// Global vars:
var (
	queryPool           = &query.HTTPPool
	queryChan           chan query.Query
	workersGroup        sync.WaitGroup
	benchmarkComponents *benchmarker.BenchmarkComponents
)

// Parse args:
func init() {
	benchmarkComponents = benchmarker.NewBenchmarkComponents()
	var csvDaemonUrls string

	flag.StringVar(&csvDaemonUrls, "urls", "http://localhost:8086", "Daemon URLs, comma-separated. Will be used in a round-robin fashion.")
	flag.StringVar(&databaseName, "db-name", "benchmark", "Name of database to use for queries")
	flag.IntVar(&workers, "workers", 1, "Number of concurrent requests to make.")
	flag.IntVar(&debug, "debug", 0, "Whether to print debug messages.")
	flag.BoolVar(&prettyPrintResponses, "print-responses", false, "Pretty print JSON response bodies (for correctness checking) (default false).")
	flag.Uint64Var(&chunkSize, "chunk-response-size", 0, "Number of series to chunk results into. 0 means no chunking.")
	flag.StringVar(&memProfile, "memprofile", "", "Write a memory profile to this file.")

	flag.Parse()

	daemonUrls = strings.Split(csvDaemonUrls, ",")
	if len(daemonUrls) == 0 {
		log.Fatal("missing 'urls' flag")
	}
}

func main() {
	// Make data and control channels:
	queryChan = make(chan query.Query, workers)

	// Launch the stats processor:
	go benchmarkComponents.StatProcessor.Process(workers)

	// Launch the query processors:
	for i := 0; i < workers; i++ {
		daemonUrl := daemonUrls[i%len(daemonUrls)]
		workersGroup.Add(1)
		w := NewHTTPClient(daemonUrl)
		go processQueries(w)
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
func processQueries(w *HTTPClient) {
	opts := &HTTPClientDoOptions{
		Debug:                debug,
		PrettyPrintResponses: prettyPrintResponses,
		chunkSize:            chunkSize,
		database:             databaseName,
	}

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
	workersGroup.Done()
}
