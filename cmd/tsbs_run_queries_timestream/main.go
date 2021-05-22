// tsbs_run_queries_timestream speed tests Timestream using requests from stdin or file
//
// It reads encoded Query objects from stdin or file, and makes concurrent requests
// to the a Timestream database encoded in the queries themselves, only the AWS region is
// required, and valid AWS credentials to be stored in .aws/credentials.
// This program has no knowledge of the internals of the endpoint.
package main

import (
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/service/timestreamquery"
	"github.com/timescale/tsbs/pkg/targets/timestream"
	"time"

	"github.com/blagojts/viper"
	_ "github.com/jackc/pgx/v4/stdlib"
	_ "github.com/lib/pq"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/pkg/query"
)

// Program option vars:
var (
	awsRegion    string
	queryTimeout time.Duration
)

// Global vars:
var (
	runner *query.BenchmarkRunner
)

// Parse args:
func init() {
	var config query.BenchmarkRunnerConfig
	config.AddToFlagSet(pflag.CommandLine)

	pflag.String("aws-region", "us-east-1", "Region where the database is")
	pflag.Duration("query-timeout", time.Minute, "Configuration for aws sdk client to timeout after")
	pflag.Parse()

	err := utils.SetupConfigFile()
	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}

	if err := viper.Unmarshal(&config); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}

	awsRegion = viper.GetString("aws-region")
	queryTimeout = viper.GetDuration("query-timeout")
	runner = query.NewBenchmarkRunner(config)
}

func main() {
	runner.Run(&query.TimestreamPool, newProcessor)
}

// prettyPrintResponse prints a Query and its response in JSON format with two
// keys: 'query' which has a value of the SQL used to generate the second key
// 'results' which is an array of each row in the return set.
func prettyPrintResponse(qry string, page *timestreamquery.QueryOutput, pageNum int) {
	resp := make(map[string]interface{})
	resp["query"] = qry
	resp["results"] = mapRows(page)
	resp["page"] = pageNum

	line, err := json.MarshalIndent(resp, "", "  ")
	if err != nil {
		panic(err)
	}

	fmt.Println(string(line) + "\n")
}

func mapRows(page *timestreamquery.QueryOutput) []map[string]string {
	var rows []map[string]string
	cols := page.ColumnInfo
	for _, row := range page.Rows {
		rowAsMap := make(map[string]string)
		for i, val := range row.Data {
			colName := cols[i].Name
			rowAsMap[*colName] = val.String()
		}

		rows = append(rows, rowAsMap)
	}
	return rows
}

type queryExecutorOptions struct {
	showExplain   bool
	debug         bool
	printResponse bool
}

type processor struct {
	_opts    *queryExecutorOptions
	_readSvc *timestreamquery.TimestreamQuery
}

func newProcessor() query.Processor {
	return &processor{}
}

func (p *processor) Init(_ int) {
	awsSession, err := timestream.OpenAWSSession(&awsRegion, queryTimeout)
	if err != nil {
		panic("could not open aws session")
	}
	p._readSvc = timestreamquery.New(awsSession)
	p._opts = &queryExecutorOptions{
		debug:         runner.DebugLevel() > 0,
		printResponse: runner.DoPrintResponses(),
	}
}

func (p *processor) ProcessQuery(q query.Query, _ bool) ([]*query.Stat, error) {
	tq := q.(*query.Timestream)

	start := time.Now()
	qry := string(tq.SqlQuery)

	if p._opts.debug {
		fmt.Println(qry)
	}

	queryInput := &timestreamquery.QueryInput{
		QueryString: &qry,
	}
	totalRows := 0
	pageNum := 1
	err := p._readSvc.QueryPages(queryInput,
		func(page *timestreamquery.QueryOutput, lastPage bool) bool {
			// process query response
			// making sure all the returned data is read
			totalRows += len(page.Rows)
			if p._opts.printResponse {
				prettyPrintResponse(qry, page, pageNum)
			}
			pageNum++
			// return true to continue to next page
			return true
		})
	if err != nil {
		return nil, err
	}
	if p._opts.debug {
		fmt.Printf("Total rows: %d\n", totalRows)
	}
	took := float64(time.Since(start).Nanoseconds()) / 1e6
	stat := query.GetStat()
	stat.Init(q.HumanLabelName(), took)

	return []*query.Stat{stat}, err
}
