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
	benchmarkRunner.Run(&query.HTTPPool, newProcessor)
}

type processor struct {
	w    *HTTPClient
	opts *HTTPClientDoOptions
}

func newProcessor() query.Processor { return &processor{} }

func (p *processor) Init(workerNumber int) {
	p.opts = &HTTPClientDoOptions{
		Debug:                benchmarkRunner.DebugLevel(),
		PrettyPrintResponses: benchmarkRunner.DoPrintResponses(),
		chunkSize:            chunkSize,
		database:             databaseName,
	}
	url := daemonUrls[workerNumber%len(daemonUrls)]
	p.w = NewHTTPClient(url)
}

func (p *processor) ProcessQuery(q query.Query, _ bool) ([]*query.Stat, error) {
	hq := q.(*query.HTTP)
	lag, err := p.w.Do(hq, p.opts)
	if err != nil {
		return nil, err
	}
	stat := query.GetStat()
	stat.Init(q.HumanLabelName(), lag)
	return []*query.Stat{stat}, nil
}
