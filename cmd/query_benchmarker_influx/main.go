// query_benchmarker speed tests InfluxDB using requests from stdin.
//
// It reads encoded Query objects from stdin, and makes concurrent requests
// to the provided HTTP endpoint. This program has no knowledge of the
// internals of the endpoint.
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
	"strings"
	"sync"
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/benchmarker"
	"bitbucket.org/440-labs/influxdb-comparisons/query"
	"github.com/influxdata/influxdb-comparisons/util/telemetry"
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
	telemetryHost        string
	telemetryStderr      bool
	telemetryBatchSize   uint64
	telemetryTagsCSV     string
	telemetryBasicAuth   string
)

// Global vars:
var (
	queryPool           = &query.HTTPPool
	queryChan           chan query.Query
	workersGroup        sync.WaitGroup
	statProcessor       *benchmarker.StatProcessor
	telemetryChanPoints chan *telemetry.Point
	telemetryChanDone   chan struct{}
	telemetrySrcAddr    string
	telemetryTags       [][2]string
)

// Parse args:
func init() {
	statProcessor = benchmarker.NewStatProcessor()
	var csvDaemonUrls string

	flag.StringVar(&csvDaemonUrls, "urls", "http://localhost:8086", "Daemon URLs, comma-separated. Will be used in a round-robin fashion.")
	flag.StringVar(&databaseName, "db-name", "benchmark", "Name of database to use for queries")
	flag.IntVar(&workers, "workers", 1, "Number of concurrent requests to make.")
	flag.IntVar(&debug, "debug", 0, "Whether to print debug messages.")
	flag.BoolVar(&prettyPrintResponses, "print-responses", false, "Pretty print JSON response bodies (for correctness checking) (default false).")
	flag.Uint64Var(&chunkSize, "chunk-response-size", 0, "Number of series to chunk results into. 0 means no chunking.")
	flag.StringVar(&memProfile, "memprofile", "", "Write a memory profile to this file.")
	flag.StringVar(&telemetryHost, "telemetry-host", "", "InfluxDB host to write telegraf telemetry to (optional).")
	flag.StringVar(&telemetryTagsCSV, "telemetry-tags", "", "Tag(s) for telemetry. Format: key0:val0,key1:val1,...")
	flag.StringVar(&telemetryBasicAuth, "telemetry-basic-auth", "", "basic auth (username:password) for telemetry.")
	flag.BoolVar(&telemetryStderr, "telemetry-stderr", false, "Whether to write telemetry also to stderr.")
	flag.Uint64Var(&telemetryBatchSize, "telemetry-batch-size", 1000, "Telemetry batch size (lines).")

	flag.Parse()

	daemonUrls = strings.Split(csvDaemonUrls, ",")
	if len(daemonUrls) == 0 {
		log.Fatal("missing 'urls' flag")
	}

	if telemetryHost != "" {
		fmt.Printf("telemetry destination: %v\n", telemetryHost)
		if telemetryBatchSize == 0 {
			panic("invalid telemetryBatchSize")
		}

		var err error
		telemetrySrcAddr, err = os.Hostname()
		if err != nil {
			log.Fatalf("os.Hostname() error: %s", err.Error())
		}
		fmt.Printf("src addr for telemetry: %v\n", telemetrySrcAddr)

		if telemetryTagsCSV != "" {
			pairs := strings.Split(telemetryTagsCSV, ",")
			for _, pair := range pairs {
				fields := strings.SplitN(pair, ":", 2)
				tagpair := [2]string{fields[0], fields[1]}
				telemetryTags = append(telemetryTags, tagpair)
			}
		}
		fmt.Printf("telemetry tags: %v\n", telemetryTags)
	}
}

func main() {
	// Make data and control channels:
	queryChan = make(chan query.Query, workers)

	// Launch the stats processor:
	go statProcessor.Process(workers)

	if telemetryHost != "" {
		telemetryCollector := telemetry.NewCollector(telemetryHost, "telegraf", telemetryBasicAuth)
		telemetryChanPoints, telemetryChanDone = telemetry.EZRunAsync(telemetryCollector, telemetryBatchSize, telemetryStderr, statProcessor.BurnIn)
	}

	// Launch the query processors:
	for i := 0; i < workers; i++ {
		daemonUrl := daemonUrls[i%len(daemonUrls)]
		workersGroup.Add(1)
		w := NewHTTPClient(daemonUrl)
		go processQueries(w, telemetryChanPoints, fmt.Sprintf("%d", i))
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

	if telemetryHost != "" {
		fmt.Println("shutting down telemetry...")
		close(telemetryChanPoints)
		<-telemetryChanDone
		fmt.Println("done shutting down telemetry.")
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

	n := uint64(0)
	for {
		if statProcessor.Limit >= 0 && n >= statProcessor.Limit {
			break
		}

		q := queryPool.Get().(*query.HTTP)
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

// processQueries reads byte buffers from queryChan and writes them to the
// target server, while tracking latency.
func processQueries(w *HTTPClient, telemetrySink chan *telemetry.Point, telemetryWorkerLabel string) {
	opts := &HTTPClientDoOptions{
		Debug:                debug,
		PrettyPrintResponses: prettyPrintResponses,
		chunkSize:            chunkSize,
		database:             databaseName,
	}
	var queriesSeen int64
	for q := range queryChan {
		ts := time.Now().UnixNano()

		lagMillis, err := w.Do(q.(*query.HTTP), opts)
		if err != nil {
			log.Fatalf("Error during request: %s\n", err.Error())
		}
		stat := statProcessor.GetStat()
		stat.Init(q.HumanLabelName(), lagMillis)
		statProcessor.C <- stat

		// Report telemetry, if applicable:
		if telemetrySink != nil {
			p := telemetry.GetPointFromGlobalPool()
			p.Init("benchmark_query", ts)
			for _, tagpair := range telemetryTags {
				p.AddTag(tagpair[0], tagpair[1])
			}
			p.AddTag("src_addr", telemetrySrcAddr)
			p.AddTag("dst_addr", w.HostString)
			p.AddTag("worker_id", telemetryWorkerLabel)
			p.AddFloat64Field("rtt_ms", lagMillis)
			p.AddInt64Field("worker_req_num", queriesSeen)
			telemetrySink <- p
		}
		queriesSeen++

		// Warm run
		lagMillis, err = w.Do(q.(*query.HTTP), opts)
		if err != nil {
			log.Fatalf("Error during request: %s\n", err.Error())
		}
		stat = statProcessor.GetStat()
		stat.InitWarm(q.HumanLabelName(), lagMillis)
		statProcessor.C <- stat

		queryPool.Put(q)
	}
	workersGroup.Done()
}
