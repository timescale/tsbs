package main

import (
	"fmt"
	"sort"
	"time"

	"github.com/gocql/gocql"
)

// A QueryPlan is a strategy used to fulfill an HLQuery.
type QueryPlan interface {
	Execute(*gocql.Session) ([]CQLResult, error)
	DebugQueries(int)
}

// A QueryPlanWithServerAggregation fulfills an HLQuery by performing
// aggregation on both the server and the client. This results in more
// round-trip requests, but uses the server to aggregate over large datasets.
//
// It has 1) an AggrFunc, which merges data on the client, and 2) a map of
// time interval buckets to CQL queries, which are used to retrieve data
// relevant to each bucket.
type QueryPlanWithServerAggregation struct {
	Aggregator         AggrFunc
	BucketedCQLQueries map[TimeInterval][]CQLQuery
}

// NewQueryPlanWithServerAggregation builds a QueryPlanWithServerAggregation.
// It is typically called via (*HLQuery).ToQueryPlanWithServerAggregation.
func NewQueryPlanWithServerAggregation(aggrLabel string, bucketedCQLQueries map[TimeInterval][]CQLQuery) (*QueryPlanWithServerAggregation, error) {
	aggr, err := GetAggrFunc(aggrLabel)
	if err != nil {
		return nil, err
	}

	qp := &QueryPlanWithServerAggregation{
		Aggregator:         aggr,
		BucketedCQLQueries: bucketedCQLQueries,
	}
	return qp, nil
}

// Execute runs all CQLQueries in the QueryPlan and collects the results.
//
// TODO(rw): support parallel execution.
func (qp *QueryPlanWithServerAggregation) Execute(session *gocql.Session) ([]CQLResult, error) {
	// sort the time interval buckets we'll use:
	sortedKeys := make([]TimeInterval, 0, len(qp.BucketedCQLQueries))
	for k := range qp.BucketedCQLQueries {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Sort(TimeIntervals(sortedKeys))

	// for each bucket, execute its queries, aggregate the results, then
	// append them to the result set:
	results := make([]CQLResult, 0, len(qp.BucketedCQLQueries))
	for _, k := range sortedKeys {
		cqlGroup := qp.BucketedCQLQueries[k]
		acc := make([]float64, 0, len(cqlGroup))
		for _, q := range cqlGroup {
			// Execute one CQLQuery and collect its result
			//
			// For server-side aggregation, this will return only
			// one row; for exclusive client-side aggregation this
			// will return a sequence.
			iter := session.Query(q.PreparableQueryString, q.Args...).Iter()
			var x float64
			for iter.Scan(&x) {
				acc = append(acc, x)
			}
			if err := iter.Close(); err != nil {
				return nil, err
			}
		}
		groupResult := qp.Aggregator(acc)
		results = append(results, CQLResult{TimeInterval: k, Value: groupResult})
	}

	return results, nil
}

// DebugQueries prints debugging information.
func (qp *QueryPlanWithServerAggregation) DebugQueries(level int) {
	if level >= 1 {
		n := 0
		for _, qq := range qp.BucketedCQLQueries {
			n += len(qq)
		}
		fmt.Printf("[qpsa] query with server aggregation plan has %d CQLQuery objects\n", n)
	}

	if level >= 2 {
		for k, qq := range qp.BucketedCQLQueries {
			for i, q := range qq {
				fmt.Printf("[qpsa] CQL: %s, %d, %s\n", k, i, q)
			}
		}
	}
}

// A QueryPlanWithoutServerAggregation fulfills an HLQuery by performing
// table scans on the server and aggregating all data on the client. This
// results in higher bandwidth usage but fewer round-trip requests.
//
// It has 1) an AggrFunc, which merges data on the client, 2) a
// GroupByDuration, which is used to reconstuct time buckets from a server
// response, 3) a set of TimeBuckets, which are used to store final
// aggregated items, and 4) a set of CQLQueries used to fulfill this plan.
type QueryPlanWithoutServerAggregation struct {
	Aggregator      AggrFunc
	GroupByDuration time.Duration
	TimeBuckets     []TimeInterval
	CQLQueries      []CQLQuery
}

// NewQueryPlanWithoutServerAggregation builds a QueryPlanWithoutServerAggregation.
// It is typically called via (*HLQuery).ToQueryPlanWithoutServerAggregation.
func NewQueryPlanWithoutServerAggregation(aggrLabel string, groupByDuration time.Duration, timeBuckets []TimeInterval, cqlQueries []CQLQuery) (*QueryPlanWithoutServerAggregation, error) {
	aggr, err := GetAggrFunc(aggrLabel)
	if err != nil {
		return nil, err
	}

	qp := &QueryPlanWithoutServerAggregation{
		Aggregator:      aggr,
		GroupByDuration: groupByDuration,
		TimeBuckets:     timeBuckets,
		CQLQueries:      cqlQueries,
	}
	return qp, nil
}

// Execute runs all CQLQueries in the QueryPlan and collects the results.
//
// TODO(rw): use constant-space acc functions (instead of requiring a slice).
// TODO(rw): support parallel execution.
func (qp *QueryPlanWithoutServerAggregation) Execute(session *gocql.Session) ([]CQLResult, error) {
	toAggregate := map[TimeInterval][]float64{}

	// for each query, execute it, then append each result row to the
	// client-side bucket that matches its timestamp:
	for _, q := range qp.CQLQueries {
		iter := session.Query(q.PreparableQueryString, q.Args...).Iter()

		var timestamp_ns int64
		var value float64

		for iter.Scan(&timestamp_ns, &value) {
			ts := time.Unix(0, timestamp_ns).UTC()
			tsTruncated := ts.Truncate(qp.GroupByDuration)
			bucketKey := TimeInterval{
				Start: tsTruncated,
				End:   tsTruncated.Add(qp.GroupByDuration),
			}

			toAggregate[bucketKey] = append(toAggregate[bucketKey], value)
		}
		if err := iter.Close(); err != nil {
			return nil, err
		}
	}

	// perform aggregation across all buckets, entirely on the client:
	results := make([]CQLResult, 0, len(qp.TimeBuckets))
	for _, ti := range qp.TimeBuckets {
		acc := qp.Aggregator(toAggregate[ti])
		results = append(results, CQLResult{TimeInterval: ti, Value: acc})
	}

	return results, nil
}

// DebugQueries prints debugging information.
func (qp *QueryPlanWithoutServerAggregation) DebugQueries(level int) {
	if level >= 1 {
		fmt.Printf("[qpca] query with client aggregation plan has %d CQLQuery objects\n", len(qp.CQLQueries))
	}

	if level >= 2 {
		for i, q := range qp.CQLQueries {
			fmt.Printf("[qpca] CQL: %d, %s\n", i, q)
		}
	}
}
