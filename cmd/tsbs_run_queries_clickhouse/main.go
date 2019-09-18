// tsbs_run_queries_clickhouse speed tests ClickHouse using requests from stdin or file.
//
// It reads encoded Query objects from stdin or file, and makes concurrent requests to the provided ClickHouse endpoint.
// This program has no knowledge of the internals of the endpoint.
package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/kshvakov/clickhouse"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/query"
)

// Program option vars:
var (
	chConnect string
	hostsList []string
	user      string
	password  string

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
	var hosts string

	pflag.String("additional-params", "sslmode=disable",
		"String of additional ClickHouse connection parameters, e.g., 'sslmode=disable'.")
	pflag.String("hosts", "localhost",
		"Comma separated list of ClickHouse hosts (pass multiple values for sharding reads on a multi-node setup)")
	pflag.String("user", "default", "User to connect to ClickHouse as")
	pflag.String("password", "", "Password to connect to ClickHouse")

	pflag.Parse()

	err := utils.SetupConfigFile()

	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}

	if err := viper.Unmarshal(&config); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}

	chConnect = viper.GetString("additional-params")
	hosts = viper.GetString("hosts")
	user = viper.GetString("user")
	password = viper.GetString("password")

	// Parse comma separated string of hosts and put in a slice (for multi-node setups)
	for _, host := range strings.Split(hosts, ",") {
		hostsList = append(hostsList, host)
	}

	runner = query.NewBenchmarkRunner(config)
}

func main() {
	runner.Run(&query.ClickHousePool, newProcessor)
}

// Get the connection string for a connection to PostgreSQL.

// If we're running queries against multiple nodes we need to balance the queries
// across replicas. Each worker is assigned a sequence number -- we'll use that
// to evenly distribute hosts to worker connections
func getConnectString(workerNumber int) string {
	// Round robin the host/worker assignment by assigning a host based on workerNumber % totalNumberOfHosts
	host := hostsList[workerNumber%len(hostsList)]

	return fmt.Sprintf("tcp://%s:9000?username=%s&password=%s&database=%s", host, user, password, runner.DatabaseName())
}

// prettyPrintResponse prints a Query and its response in JSON format with two
// keys: 'query' which has a value of the SQL used to generate the second key
// 'results' which is an array of each row in the return set.
func prettyPrintResponse(rows *sqlx.Rows, q *query.ClickHouse) {
	resp := make(map[string]interface{})
	resp["query"] = string(q.SqlQuery)

	results := []map[string]interface{}{}
	for rows.Next() {
		r := make(map[string]interface{})
		if err := rows.MapScan(r); err != nil {
			panic(err)
		}
		results = append(results, r)
		resp["results"] = results
	}

	line, err := json.MarshalIndent(resp, "", "  ")
	if err != nil {
		panic(err)
	}

	fmt.Println(string(line) + "\n")
}

type queryExecutorOptions struct {
	showExplain   bool
	debug         bool
	printResponse bool
}

// query.Processor interface implementation
type processor struct {
	db   *sqlx.DB
	opts *queryExecutorOptions
}

// query.Processor interface implementation
func newProcessor() query.Processor {
	return &processor{}
}

// query.Processor interface implementation
func (p *processor) Init(workerNumber int) {
	p.db = sqlx.MustConnect("clickhouse", getConnectString(workerNumber))
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
	chQuery := q.(*query.ClickHouse)

	start := time.Now()

	// SqlQuery is []byte, so cast is needed
	sql := string(chQuery.SqlQuery)

	// Main action - run the query
	rows, err := p.db.Queryx(sql)
	if err != nil {
		return nil, err
	}

	// Print some extra info if needed
	if p.opts.debug {
		fmt.Println(sql)
	}
	if p.opts.printResponse {
		prettyPrintResponse(rows, chQuery)
	}

	// Finalize the query
	rows.Close()
	took := float64(time.Since(start).Nanoseconds()) / 1e6

	stat := query.GetStat()
	stat.Init(q.HumanLabelName(), took)

	return []*query.Stat{stat}, err
}
