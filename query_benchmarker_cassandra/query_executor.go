package main

import (
	"fmt"
	"os"
	"time"

	"github.com/gocql/gocql"
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
	var qp *QueryPlan
	qpStart := time.Now()
	qp, err = q.ToQueryPlan(qe.csi)
	qpLag := time.Now().Sub(qpStart).Seconds()
	if opts.Debug >= 1 {
		// FYI: query planning takes about 0.5ms for 1000 series.
		fmt.Printf("[hlqe] query planning took %fs\n", qpLag)

		n := 0
		for _, qq := range qp.BucketedCQLQueries {
			n += len(qq)
		}
		fmt.Printf("[hlqe] query plan has %d CQLQuery objects\n", n)
	}

	if opts.Debug >= 2 {
		for k, qq := range qp.BucketedCQLQueries {
			for i, q := range qq {
				fmt.Printf("[hlqe] CQL: %s, %d, %s\n", k, i, q)
			}
		}
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
