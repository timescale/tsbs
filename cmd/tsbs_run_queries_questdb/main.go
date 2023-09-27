// tsbs_run_queries_influx speed tests InfluxDB using requests from stdin.
//
// It reads encoded Query objects from stdin, and makes concurrent requests
// to the provided HTTP endpoint. This program has no knowledge of the
// internals of the endpoint.
package main

import (
	"fmt"

	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/pkg/query"
)

// Program option vars:
var (
	restURL  string
	username string
	password string
)

// Global vars:
var (
	runner *query.BenchmarkRunner
)

// Parse args:
func init() {
	var config query.BenchmarkRunnerConfig
	config.AddToFlagSet(pflag.CommandLine)

	pflag.String("url", "http://localhost:9000/", "Server URL. In case of HTTPS, the client will not validate the certificate, i.e. it trusts any server")
	pflag.String("username", "", "Basic auth username")
	pflag.String("password", "", "Basic auth password")

	pflag.Parse()

	err := utils.SetupConfigFile()

	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}

	if err := viper.Unmarshal(&config); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}

	restURL = viper.GetString("url")
	username = viper.GetString("username")
	password = viper.GetString("password")

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
		Username:             username,
		Password:             password,
		Debug:                runner.DebugLevel(),
		PrettyPrintResponses: runner.DoPrintResponses(),
	}
	p.w = NewHTTPClient(restURL)
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
