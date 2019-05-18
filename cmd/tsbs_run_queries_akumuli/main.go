// tsbs_run_queries_akumuli speed tests Akumuli using requests from stdin.
//
// It reads encoded Query objects from stdin, and makes concurrent requests
// to the provided HTTP endpoint. This program has no knowledge of the
// internals of the endpoint.
package main

import (
	"flag"

	"github.com/timescale/tsbs/query"
)

// Program option vars:
var (
	endpoint string
)

// Global vars:
var (
	runner *query.BenchmarkRunner
)

// Parse args:
func init() {
	runner = query.NewBenchmarkRunner()

	flag.StringVar(&endpoint, "endpoint", "http://localhost:8181", "Akumuli API endpoint IP address.")

	flag.Parse()
}

func main() {
	runner.Run(&query.HTTPPool, newProcessor)
}

type processor struct {
	w    *HTTPClient
	opts *HTTPClientDoOptions
}

func newProcessor() query.Processor { return &processor{} }

func (p *processor) Init(workerNumber int) {
	p.opts = &HTTPClientDoOptions{
		Debug:          runner.DebugLevel(),
		PrintResponses: runner.DoPrintResponses(),
	}
	url := endpoint
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
