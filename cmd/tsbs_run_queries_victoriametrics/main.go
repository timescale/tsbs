// tsbs_run_queries_victoriametrics speed tests VictoriaMetrics using requests from stdin or file.
//
// It reads encoded Query objects from stdin, and makes concurrent requests
// to the provided HTTP endpoint. This program has no knowledge of the
// internals of the endpoint.
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/pkg/query"
)

// Program option vars:
var (
	vmURLs []string
)

// Global vars:
var (
	runner *query.BenchmarkRunner
)

// Parse args:
func init() {
	var config query.BenchmarkRunnerConfig
	config.AddToFlagSet(pflag.CommandLine)

	pflag.String("urls", "http://localhost:8428",
		"Comma-separated list of VictoriaMetrics ingestion URLs(single-node or VMSelect)")

	pflag.Parse()

	if err := utils.SetupConfigFile(); err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}
	if err := viper.Unmarshal(&config); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}

	urls := viper.GetString("urls")
	if len(urls) == 0 {
		log.Fatalf("missing `urls` flag")
	}
	vmURLs = strings.Split(urls, ",")
	runner = query.NewBenchmarkRunner(config)
}

func main() {
	runner.Run(&query.HTTPPool, newProcessor)
}

func newProcessor() query.Processor {
	return &processor{}
}

// query.Processor interface implementation
type processor struct {
	url string

	prettyPrintResponses bool
}

// query.Processor interface implementation
func (p *processor) Init(workerNum int) {
	p.url = vmURLs[workerNum%len(vmURLs)]
	p.prettyPrintResponses = runner.DoPrintResponses()
}

// query.Processor interface implementation
func (p *processor) ProcessQuery(q query.Query, isWarm bool) ([]*query.Stat, error) {
	hq := q.(*query.HTTP)
	lag, err := p.do(hq)
	if err != nil {
		return nil, err
	}
	stat := query.GetStat()
	stat.Init(q.HumanLabelName(), lag)
	return []*query.Stat{stat}, nil
}

func (p *processor) do(q *query.HTTP) (float64, error) {
	// populate a request with data from the Query:
	req, err := http.NewRequest(string(q.Method), p.url+string(q.Path), nil)
	if err != nil {
		return 0, fmt.Errorf("error while creating request: %s", err)
	}

	start := time.Now()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("query execution error: %s", err)
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("error while reading response body: %s", err)
	}
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("non-200 statuscode received: %d; Body: %s", resp.StatusCode, string(body))
	}
	lag := float64(time.Since(start).Nanoseconds()) / 1e6 // milliseconds

	// Pretty print JSON responses, if applicable:
	if p.prettyPrintResponses {
		var pretty bytes.Buffer
		prefix := fmt.Sprintf("ID %d: ", q.GetID())
		if err := json.Indent(&pretty, body, prefix, "  "); err != nil {
			return lag, err
		}
		_, err = fmt.Fprintf(os.Stderr, "%s%s\n", prefix, pretty.Bytes())
		if err != nil {
			return lag, err
		}
	}
	return lag, nil
}
