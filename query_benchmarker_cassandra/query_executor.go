package main

import (
	"fmt"
	"os"
	"time"

	"github.com/gocql/gocql"
)

const (
	AggrPlanTypeServerAggregation   = 1
	AggrPlanTypeNoServerAggregation = 2
)

// An HLQueryExecutor is responsible for executing HLQuery objects in the
// context of a particular Cassandra session and data set.
type HLQueryExecutor struct {
	session *gocql.Session
	csi     *ClientSideIndex
	debug   int
}

// NewHLQueryExecutor creates an HLQueryExecutor from a ClientSideIndex and
// Cassandra session.
func NewHLQueryExecutor(session *gocql.Session, csi *ClientSideIndex, debug int) *HLQueryExecutor {
	return &HLQueryExecutor{
		session: session,
		csi:     csi,
		debug:   debug,
	}
}

// HLQueryExecutorDoOptions contains options used by HLQueryExecutor.
type HLQueryExecutorDoOptions struct {
	AggregationPlan      int
	SubQueryParallelism  int // unused
	Debug                int
	PrettyPrintResponses bool
}

// Do takes a high-level query, constructs a query plan using the client-side
// index contained within the query executor, executes that query plan, then
// aggregates the results.
func (qe *HLQueryExecutor) Do(q *HLQuery, opts HLQueryExecutorDoOptions) (lagMs float64, err error) {
	if opts.Debug >= 1 {
		fmt.Printf("[hlqe] Do: %s\n", q)
	}

	// build the query plan:
	var qp QueryPlan
	qpStart := time.Now()
	switch opts.AggregationPlan {
	case AggrPlanTypeServerAggregation:
		qp, err = q.ToQueryPlanWithServerAggregation(qe.csi)
	case AggrPlanTypeNoServerAggregation:
		qp, err = q.ToQueryPlanWithoutServerAggregation(qe.csi)
	default:
		panic("logic error: invalid aggregation plan option")
	}
	qpLag := time.Now().Sub(qpStart).Seconds()

	// print debug info if needed:
	if opts.Debug >= 1 {
		// FYI: query planning takes about 0.5ms for 1000 series.
		fmt.Printf("[hlqe] query planning took %fs\n", qpLag)

		qp.DebugQueries(opts.Debug)
	}

	if err != nil {
		return
	}

	// execute the query plan:
	var results []CQLResult
	execStart := time.Now()
	results, err = qp.Execute(qe.session)
	lagMs = float64(time.Now().Sub(execStart).Nanoseconds()) / 1e6
	if err != nil {
		return lagMs, err
	}

	// optionally, print reponses for query validation:
	if opts.PrettyPrintResponses {
		for _, r := range results {
			fmt.Fprintf(os.Stderr, "ID %d: [%s, %s] -> %f\n", q.ID, r.TimeInterval.Start, r.TimeInterval.End, r.Value)
		}
	}
	return
}
