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
// It has 1) an Aggregator, which merges data on the client, and 2) a map of
// time interval buckets to CQL queries, which are used to retrieve data
// relevant to each bucket.
type QueryPlanWithServerAggregation struct {
	AggregatorLabel    string
	BucketedCQLQueries map[TimeInterval][]CQLQuery
}

// NewQueryPlanWithServerAggregation builds a QueryPlanWithServerAggregation.
// It is typically called via (*HLQuery).ToQueryPlanWithServerAggregation.
func NewQueryPlanWithServerAggregation(aggrLabel string, bucketedCQLQueries map[TimeInterval][]CQLQuery) (*QueryPlanWithServerAggregation, error) {
	qp := &QueryPlanWithServerAggregation{
		AggregatorLabel:    aggrLabel,
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

	// for each bucket, execute its queries while aggregating its results
	// in constant space, then append them to the result set:
	results := make([]CQLResult, 0, len(qp.BucketedCQLQueries))
	for _, k := range sortedKeys {
		agg, err := GetAggregator(qp.AggregatorLabel)
		if err != nil {
			return nil, err
		}

		for _, q := range qp.BucketedCQLQueries[k] {
			// Execute one CQLQuery and collect its result
			//
			// For server-side aggregation, this will return only
			// one row; for exclusive client-side aggregation this
			// will return a sequence.
			iter := session.Query(q.PreparableQueryString, q.Args...).Iter()
			var x float64
			for iter.Scan(&x) {
				agg.Put(x)
			}
			if err := iter.Close(); err != nil {
				return nil, err
			}
		}
		results = append(results, CQLResult{TimeInterval: k, Values: []float64{agg.Get()}})
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
// It has 1) a map of Aggregators (one for each time bucket) which merge data
// on the client, 2) a GroupByDuration, which is used to reconstruct time
// buckets from a server response, 3) a set of TimeBuckets, which are used to
// store final aggregated items, and 4) a set of CQLQueries used to fulfill
// this plan.
type QueryPlanWithoutServerAggregation struct {
	Aggregators     map[TimeInterval]map[string]Aggregator
	GroupByDuration time.Duration
	Fields          []string
	TimeBuckets     []TimeInterval
	CQLQueries      []CQLQuery
}

// NewQueryPlanWithoutServerAggregation builds a QueryPlanWithoutServerAggregation.
// It is typically called via (*HLQuery).ToQueryPlanWithoutServerAggregation.
func NewQueryPlanWithoutServerAggregation(aggrLabel string, groupByDuration time.Duration, fields []string, timeBuckets []TimeInterval, cqlQueries []CQLQuery) (*QueryPlanWithoutServerAggregation, error) {
	aggrs := make(map[TimeInterval]map[string]Aggregator, len(timeBuckets))
	for _, ti := range timeBuckets {
		aggrs[ti] = make(map[string]Aggregator)
		for _, f := range fields {
			aggr, err := GetAggregator(aggrLabel)
			if err != nil {
				return nil, err
			}

			aggrs[ti][f] = aggr
		}
	}

	qp := &QueryPlanWithoutServerAggregation{
		Aggregators:     aggrs,
		GroupByDuration: groupByDuration,
		Fields:          fields,
		TimeBuckets:     timeBuckets,
		CQLQueries:      cqlQueries,
	}
	return qp, nil
}

// Execute runs all CQLQueries in the QueryPlan and collects the results.
//
// TODO(rw): support parallel execution.
func (qp *QueryPlanWithoutServerAggregation) Execute(session *gocql.Session) ([]CQLResult, error) {
	// for each query, execute it, then put each result row into the
	// client-side aggregator that matches its time bucket:
	for _, q := range qp.CQLQueries {
		iter := session.Query(q.PreparableQueryString, q.Args...).Iter()

		var timestampNs int64
		var value float64

		for iter.Scan(&timestampNs, &value) {
			ts := time.Unix(0, timestampNs).UTC()
			tsTruncated := ts.Truncate(qp.GroupByDuration)
			bucketKey := TimeInterval{
				Start: tsTruncated,
				End:   tsTruncated.Add(qp.GroupByDuration),
			}

			qp.Aggregators[bucketKey][q.Field].Put(value)
		}
		if err := iter.Close(); err != nil {
			return nil, err
		}
	}

	// perform client-side aggregation across all buckets:
	results := make([]CQLResult, 0, len(qp.TimeBuckets))
	for _, ti := range qp.TimeBuckets {
		res := CQLResult{TimeInterval: ti, Values: make([]float64, len(qp.Fields))}
		for i, f := range qp.Fields {
			res.Values[i] = qp.Aggregators[ti][f].Get()
		}
		results = append(results, res)
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
