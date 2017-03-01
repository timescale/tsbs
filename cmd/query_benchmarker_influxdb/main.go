// query_benchmarker speed tests InfluxDB using requests from stdin.
//
// It reads encoded Query objects from stdin, and makes concurrent requests
// to the provided HTTP endpoint. This program has no knowledge of the
// internals of the endpoint.
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
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/influxdata/influxdb-comparisons/util/telemetry"
)

// Program option vars:
var (
	csvDaemonUrls           string
	daemonUrls              []string
	workers                 int
	debug                   int
	prettyPrintResponses    bool
	limit                   int64
	burnIn                  uint64
	printInterval           uint64
	memProfile              string
	telemetryHost           string
	telemetryStderr         bool
	telemetryBatchSize      uint64
	telemetryExperimentName string
	telemetryTagsCSV        string
	telemetryBasicAuth      string
)

// Global vars:
var (
	queryPool           sync.Pool
	queryChan           chan *Query
	statPool            sync.Pool
	statChan            chan *Stat
	workersGroup        sync.WaitGroup
	statGroup           sync.WaitGroup
	telemetryChanPoints chan *telemetry.Point
	telemetryChanDone   chan struct{}
	telemetrySrcAddr    string
	telemetryTags       [][2]string
)

// Parse args:
func init() {
	flag.StringVar(&csvDaemonUrls, "urls", "http://localhost:8086", "Daemon URLs, comma-separated. Will be used in a round-robin fashion.")
	flag.IntVar(&workers, "workers", 1, "Number of concurrent requests to make.")
	flag.IntVar(&debug, "debug", 0, "Whether to print debug messages.")
	flag.Int64Var(&limit, "limit", -1, "Limit the number of queries to send.")
	flag.Uint64Var(&burnIn, "burn-in", 0, "Number of queries to ignore before collecting statistics.")
	flag.Uint64Var(&printInterval, "print-interval", 100, "Print timing stats to stderr after this many queries (0 to disable)")
	flag.BoolVar(&prettyPrintResponses, "print-responses", false, "Pretty print JSON response bodies (for correctness checking) (default false).")
	flag.StringVar(&memProfile, "memprofile", "", "Write a memory profile to this file.")
	flag.StringVar(&telemetryHost, "telemetry-host", "", "InfluxDB host to write telegraf telemetry to (optional).")
	flag.StringVar(&telemetryExperimentName, "telemetry-experiment-name", "unnamed_experiment", "Experiment name for telemetry.")
	flag.StringVar(&telemetryTagsCSV, "telemetry-tags", "", "Tag(s) for telemetry. Format: key0:val0,key1:val1,...")
	flag.StringVar(&telemetryBasicAuth, "telemetry-basic-auth", "", "basic auth (username:password) for telemetry.")
	flag.BoolVar(&telemetryStderr, "telemetry-stderr", false, "Whether to write telemetry also to stderr.")
	flag.Uint64Var(&telemetryBatchSize, "telemetry-batch-size", 1000, "Telemetry batch size (lines).")

	flag.Parse()

	daemonUrls = strings.Split(csvDaemonUrls, ",")
	if len(daemonUrls) == 0 {
		log.Fatal("missing 'urls' flag")
	}
	fmt.Printf("daemon URLs: %v\n", daemonUrls)

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
	// Make pools to minimize heap usage:
	queryPool = sync.Pool{
		New: func() interface{} {
			return &Query{
				HumanLabel:       make([]byte, 0, 1024),
				HumanDescription: make([]byte, 0, 1024),
				Method:           make([]byte, 0, 1024),
				Path:             make([]byte, 0, 1024),
				Body:             make([]byte, 0, 1024),
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

	if telemetryHost != "" {
		telemetryCollector := telemetry.NewCollector(telemetryHost, "telegraf", telemetryBasicAuth)
		telemetryChanPoints, telemetryChanDone = telemetry.EZRunAsync(telemetryCollector, telemetryBatchSize, telemetryExperimentName, telemetryStderr, burnIn)
	}

	// Launch the query processors:
	for i := 0; i < workers; i++ {
		daemonUrl := daemonUrls[i%len(daemonUrls)]
		workersGroup.Add(1)
		w := NewHTTPClient(daemonUrl, debug)
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
	close(statChan)

	// Wait on the stat collector to finish (and print its results):
	statGroup.Wait()

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

// processQueries reads byte buffers from queryChan and writes them to the
// target server, while tracking latency.
func processQueries(w *HTTPClient, telemetrySink chan *telemetry.Point, telemetryWorkerLabel string) {
	opts := &HTTPClientDoOptions{
		Debug:                debug,
		PrettyPrintResponses: prettyPrintResponses,
	}
	var queriesSeen int64
	for q := range queryChan {
		ts := time.Now().UnixNano()
		lagMillis, err := w.Do(q, opts)

		stat := statPool.Get().(*Stat)
		stat.Init(q.HumanLabel, lagMillis)
		statChan <- stat

		queryPool.Put(q)
		if err != nil {
			log.Fatalf("Error during request: %s\n", err.Error())
		}

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
			statMapping[string(stat.Label)] = &StatGroup{}
		}

		statMapping[allQueriesLabel].Push(stat.Value)
		statMapping[string(stat.Label)].Push(stat.Value)

		statPool.Put(stat)

		i++

		// print stats to stderr (if printInterval is greater than zero):
		if printInterval > 0 && i > 0 && i%printInterval == 0 && (int64(i) < limit || limit < 0) {
			_, err := fmt.Fprintf(os.Stderr, "after %d queries with %d workers:\n", i-burnIn, workers)
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
	_, err := fmt.Printf("run complete after %d queries with %d workers:\n", i-burnIn, workers)
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
		_, err := fmt.Fprintf(w, "%s : min: %8.2fms (%7.2f/sec), mean: %8.2fms (%7.2f/sec), max: %7.2fms (%6.2f/sec), count: %8d, sum: %5.1fsec \n", paddedKey, v.Min, minRate, v.Mean, meanRate, v.Max, maxRate, v.Count, v.Sum/1e3)
		if err != nil {
			log.Fatal(err)
		}
	}

}
