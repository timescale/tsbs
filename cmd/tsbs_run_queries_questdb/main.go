// tsbs_run_queries_influx speed tests InfluxDB using requests from stdin.
//
// It reads encoded Query objects from stdin, and makes concurrent requests
// to the provided HTTP endpoint. This program has no knowledge of the
// internals of the endpoint.
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strings"

	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/pkg/query"
)

// Program option vars:
var (
	daemonUrls []string
)

// Global vars:
var (
	runner *query.BenchmarkRunner
)

// Parse args:
func init() {
	var config query.BenchmarkRunnerConfig
	config.AddToFlagSet(pflag.CommandLine)
	var csvDaemonUrls string

	pflag.String("urls", "http://localhost:9000/", "Daemon URLs, comma-separated. Will be used in a round-robin fashion.")

	pflag.Parse()

	err := utils.SetupConfigFile()

	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}

	if err := viper.Unmarshal(&config); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}

	csvDaemonUrls = viper.GetString("urls")

	daemonUrls = strings.Split(csvDaemonUrls, ",")
	if len(daemonUrls) == 0 {
		log.Fatal("missing 'urls' flag")
	}

	// Add an index to the hostname column in the cpu table
	r, err := execQuery(daemonUrls[0], "show columns from cpu")
	if err == nil && r.Count != 0 {
		r, err := execQuery(daemonUrls[0], "ALTER TABLE cpu ALTER COLUMN hostname ADD INDEX")
		_ = r
		//	       fmt.Println("error:", err)
		//	       fmt.Printf("%+v\n", r)
		if err == nil {
			fmt.Println("Added index to hostname column of cpu table")
		}
	}

	runner = query.NewBenchmarkRunner(config)
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
		Debug:                runner.DebugLevel(),
		PrettyPrintResponses: runner.DoPrintResponses(),
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

type QueryResponseColumns struct {
	Name string
	Type string
}

type QueryResponse struct {
	Query   string
	Columns []QueryResponseColumns
	Dataset []interface{}
	Count   int
	Error   string
}

func execQuery(uriRoot string, query string) (QueryResponse, error) {
	var qr QueryResponse
	if strings.HasSuffix(uriRoot, "/") {
		uriRoot = uriRoot[:len(uriRoot)-1]
	}
	uriRoot = uriRoot + "/exec?query=" + url.QueryEscape(query)
	resp, err := http.Get(uriRoot)
	if err != nil {
		return qr, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return qr, err
	}
	err = json.Unmarshal(body, &qr)
	if err != nil {
		return qr, err
	}
	if qr.Error != "" {
		return qr, errors.New(qr.Error)
	}
	return qr, nil
}
