// tsbs_run_queries_ceresdb speed tests CeresDB using requests from stdin or file.
//
// It reads encoded Query objects from stdin or file, and makes concurrent requests to the provided CeresDB endpoint.
// This program has no knowledge of the internals of the endpoint.
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/blagojts/viper"
	"github.com/jiacai2050/ceresdb_client_go/ceresdb"
	_ "github.com/kshvakov/clickhouse"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/pkg/query"
)

// Program option vars:
var (
	ceresdbAddr string

	showExplain bool
)

// Global vars:
var (
	runner *query.BenchmarkRunner
)

// Parse args:
func init() {
	var config query.BenchmarkRunnerConfig
	config.AddToFlagSet(pflag.CommandLine)

	pflag.String(
		"ceresdb-addr",
		"127.0.0.1:8831",
		"ceresdb gRPC endpoint",
	)
	pflag.Bool("show-explain", false, "Print out the EXPLAIN output for sample query")
	pflag.Parse()

	err := utils.SetupConfigFile()

	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}

	if err := viper.Unmarshal(&config); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}

	ceresdbAddr = viper.GetString("ceresdb-addr")
	showExplain = viper.GetBool("show-explain")

	runner = query.NewBenchmarkRunner(config)
}

func main() {
	runner.Run(&query.CeresDBPool, newProcessor)
}

type queryExecutorOptions struct {
	showExplain   bool
	debug         bool
	printResponse bool
}

// query.Processor interface implementation
type processor struct {
	db   *ceresdb.Client
	opts *queryExecutorOptions
}

// query.Processor interface implementation
func newProcessor() query.Processor {
	return &processor{}
}

// query.Processor interface implementation
func (p *processor) Init(workerNumber int) {
	client, err := ceresdb.NewClient(ceresdbAddr)
	if err != nil {
		panic(err)
	}
	p.db = client
	p.opts = &queryExecutorOptions{
		// ClickHouse could not do EXPLAIN
		showExplain:   false,
		debug:         runner.DebugLevel() > 0,
		printResponse: runner.DoPrintResponses(),
	}
}

// query.Processor interface implementation
func (p *processor) ProcessQuery(q query.Query, isWarm bool) ([]*query.Stat, error) {
	// No need to run again for EXPLAIN
	if isWarm && p.opts.showExplain {
		return nil, nil
	}

	// Ensure ClickHouse query
	ceresdbQuery := q.(*query.CeresDB)

	start := time.Now()

	// SqlQuery is []byte, so cast is needed
	sql := string(ceresdbQuery.SqlQuery)
	if showExplain {
		sql = "explain " + sql
	}

	// Main action - run the query
	rows, err := p.db.Query(context.TODO(), sql)
	if err != nil {
		return nil, err
	}

	// Print some extra info if needed
	if p.opts.debug {
		fmt.Println(sql)
	}
	if p.opts.printResponse {
		fmt.Printf("request = %s\n", rows)
	}

	took := float64(time.Since(start).Nanoseconds()) / 1e6

	stat := query.GetStat()
	stat.Init(q.HumanLabelName(), took)

	return []*query.Stat{stat}, err
}
