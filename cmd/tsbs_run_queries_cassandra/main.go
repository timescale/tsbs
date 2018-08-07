// tsbs_run_queries_cassandra speed tests Cassandra servers using request
// data from stdin.
//
// It reads encoded HLQuery objects from stdin, and makes concurrent requests
// to the provided Cassandra cluster. This program is a 'heavy client', i.e.
// it builds a client-side index of table metadata before beginning the
// benchmarking.
package main

import (
	"flag"
	"log"
	"time"

	"github.com/gocql/gocql"
	"github.com/timescale/tsbs/query"
)

const (
	BucketDuration   = 24 * time.Hour
	BucketTimeLayout = "2006-01-02"
)

// Blessed tables that hold benchmark data:
var (
	BlessedTables = []string{
		"series_bigint",
		"series_float",
		"series_double",
		"series_boolean",
		"series_blob",
	}
)

// Program option vars:
var (
	daemonURL      string
	aggrPlanLabel  string
	requestTimeout time.Duration
	csiTimeout     time.Duration
)

// Helpers for choice-like flags:
var (
	aggrPlanChoices = map[string]int{
		"server": AggrPlanTypeWithServerAggregation,
		"client": AggrPlanTypeWithoutServerAggregation,
	}
)

// Global vars:
var (
	runner   *query.BenchmarkRunner
	aggrPlan int
	csi      *ClientSideIndex
	session  *gocql.Session
)

// Parse args:
func init() {
	runner = query.NewBenchmarkRunner()

	flag.StringVar(&daemonURL, "host", "localhost:9042", "Cassandra hostname and port combination.")
	flag.StringVar(&aggrPlanLabel, "aggregation-plan", "", "Aggregation plan (choices: server, client)")
	flag.DurationVar(&requestTimeout, "read-timeout", 1*time.Second, "Maximum request timeout.")
	flag.DurationVar(&csiTimeout, "client-side-index-timeout", 10*time.Second, "Maximum client-side index timeout (only used at initialization).")

	flag.Parse()

	if _, ok := aggrPlanChoices[aggrPlanLabel]; !ok {
		log.Fatal("invalid aggregation plan")
	}
	aggrPlan = aggrPlanChoices[aggrPlanLabel]

}

func main() {
	// Make client-side index:
	session = NewCassandraSession(daemonURL, runner.DatabaseName(), csiTimeout)
	csi = NewClientSideIndex(FetchSeriesCollection(session))
	session.Close()

	// Make database connection pool:
	session = NewCassandraSession(daemonURL, runner.DatabaseName(), requestTimeout)
	defer session.Close()

	runner.Run(&query.CassandraPool, newProcessor)
}

type processor struct {
	qe   *HLQueryExecutor
	opts *HLQueryExecutorDoOptions
}

func newProcessor() query.Processor { return &processor{} }

func (p *processor) Init(workerNumber int) {
	p.opts = &HLQueryExecutorDoOptions{
		AggregationPlan:      aggrPlan,
		Debug:                runner.DebugLevel(),
		PrettyPrintResponses: runner.DoPrintResponses(),
	}
	p.qe = NewHLQueryExecutor(session, csi, runner.DebugLevel())
}

func (p *processor) ProcessQuery(q query.Query, isWarm bool) ([]*query.Stat, error) {
	cq := q.(*query.Cassandra)
	hlq := &HLQuery{*cq}
	hlq.ForceUTC()
	labels := [][]byte{
		q.HumanLabelName(),
		append(q.HumanLabelName(), "-qp"...),
		append(q.HumanLabelName(), "-req"...),
	}
	if isWarm {
		for i, l := range labels {
			labels[i] = append(l, " (warm)"...)
		}
	}
	qpLagMs, reqLagMs, err := p.qe.Do(hlq, *p.opts)
	if err != nil {
		return nil, err
	}
	// total stat
	totalMs := qpLagMs + reqLagMs
	stats := []*query.Stat{
		query.GetPartialStat().Init(labels[1], qpLagMs),
		query.GetPartialStat().Init(labels[2], reqLagMs),
		query.GetStat().Init(labels[0], totalMs),
	}
	return stats, nil
}
