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
	"fmt"
	"log"
	"time"

	"github.com/gocql/gocql"

	"bitbucket.org/440-labs/influxdb-comparisons/query"
)

const (
	BucketDuration   = 24 * time.Hour
	BucketTimeLayout = "2006-01-02"
	BlessedKeyspace  = "measurements"
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
	daemonUrl           string
	aggrPlanLabel       string
	subQueryParallelism int
	requestTimeout      time.Duration
	csiTimeout          time.Duration
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
	benchmarkRunner *query.BenchmarkRunner
	aggrPlan        int
	csi             *ClientSideIndex
	session         *gocql.Session
)

// Parse args:
func init() {
	benchmarkRunner = query.NewBenchmarkRunner()

	flag.StringVar(&daemonUrl, "url", "localhost:9042", "Cassandra URL.")
	flag.StringVar(&aggrPlanLabel, "aggregation-plan", "", "Aggregation plan (choices: server, client)")
	flag.IntVar(&subQueryParallelism, "subquery-workers", 1, "Number of concurrent subqueries to make (because the client does a scatter+gather operation).")
	flag.DurationVar(&requestTimeout, "request-timeout", 1*time.Second, "Maximum request timeout.")
	flag.DurationVar(&csiTimeout, "client-side-index-timeout", 10*time.Second, "Maximum client-side index timeout (only used at initialization).")

	flag.Parse()

	if _, ok := aggrPlanChoices[aggrPlanLabel]; !ok {
		log.Fatal("invalid aggregation plan")
	}
	aggrPlan = aggrPlanChoices[aggrPlanLabel]

}

func main() {
	// Make client-side index:
	csi = NewClientSideIndex(FetchSeriesCollection(daemonUrl, csiTimeout))

	// Make database connection pool:
	session = NewCassandraSession(daemonUrl, requestTimeout)
	defer session.Close()

	benchmarkRunner.Run(&query.CassandraPool, newProcessor)
}

type processor struct {
	qe     *HLQueryExecutor
	opts   *HLQueryExecutorDoOptions
	qFn    func(sp *query.StatProcessor, q *HLQuery, labels [][]byte, warm bool)
	labels map[string][][]byte
}

func newProcessor() query.Processor { return &processor{} }

func (p *processor) Init(workerNumber int) {
	p.opts = &HLQueryExecutorDoOptions{
		AggregationPlan:      aggrPlan,
		Debug:                benchmarkRunner.DebugLevel(),
		PrettyPrintResponses: benchmarkRunner.DoPrintResponses(),
	}

	p.qe = NewHLQueryExecutor(session, csi, benchmarkRunner.DebugLevel())

	p.qFn = func(sp *query.StatProcessor, q *HLQuery, labels [][]byte, warm bool) {
		qpLagMs, reqLagMs, err := p.qe.Do(q, *p.opts)
		if err != nil {
			log.Fatalf("Error during request: %s\n", err.Error())
		}

		// total stat
		totalMs := qpLagMs + reqLagMs
		if warm {
			sp.SendStat(append(labels[0], " (warm)"...), totalMs, true)
		} else {
			sp.SendStat(labels[0], totalMs, false)
		}

		// qp lag stat:
		if warm {
			sp.SendPartialStat(append(labels[1], " (warm)"...), qpLagMs, true)
		} else {
			sp.SendPartialStat(labels[1], qpLagMs, false)
		}

		// req lag stat:
		if warm {
			sp.SendPartialStat(append(labels[2], " (warm)"...), reqLagMs, true)
		} else {
			sp.SendPartialStat(labels[2], reqLagMs, false)
		}
	}

	p.labels = map[string][][]byte{}
}

func (p *processor) ProcessQuery(sp *query.StatProcessor, q query.Query) {
	cq := q.(*query.Cassandra)
	hlq := &HLQuery{*cq}
	hlq.ForceUTC()
	// if needed, prepare stat labels:
	if _, ok := p.labels[string(hlq.HumanLabel)]; !ok {
		p.labels[string(hlq.HumanLabel)] = [][]byte{
			hlq.HumanLabel,
			[]byte(fmt.Sprintf("%s-qp", hlq.HumanLabel)),
			[]byte(fmt.Sprintf("%s-req", hlq.HumanLabel)),
		}
	}
	ls := p.labels[string(hlq.HumanLabel)]

	p.qFn(sp, hlq, ls, !sp.PrewarmQueries)
	// If PrewarmQueries is set, we run the query as 'cold' first (see above),
	// then we immediately run it a second time and report that as the 'warm'
	// stat. This guarantees that the warm stat will reflect optimal cache performance.
	if sp.PrewarmQueries {
		p.qFn(sp, hlq, ls, true)
	}
}
