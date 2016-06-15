package main

import (
	"sort"

	"github.com/gocql/gocql"
)

// A QueryPlan is a concrete strategy used to fulfill an HLQuery.
//
// It has 1) an AggrFunc, which merges data on the client, and 2) a map of
// time interval buckets to CQL queries, which are used to retrieve data
// relevant to each bucket.
type QueryPlan struct {
	Aggregator         AggrFunc
	BucketedCQLQueries map[TimeInterval][]CQLQuery
}

// NewQueryPlan builds a QueryPlan. It is typically called via
// (*HLQuery).ToQueryPlan.
func NewQueryPlan(aggrLabel string, bucketedCQLQueries map[TimeInterval][]CQLQuery) (*QueryPlan, error) {
	aggr, err := GetAggrFunc(aggrLabel)
	if err != nil {
		return nil, err
	}

	qp := &QueryPlan{
		Aggregator:         aggr,
		BucketedCQLQueries: bucketedCQLQueries,
	}
	return qp, nil
}

// Execute runs all CQLQueries in the QueryPlan and collects the results.
//
// TODO(rw): support parallel execution.
func (qp *QueryPlan) Execute(session *gocql.Session) ([]CQLResult, error) {
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
		agg := make([]float64, 0, len(cqlGroup))
		for _, q := range cqlGroup {
			// execute one CQLQuery and collect its result
			// (we know that it will only return one row)
			iter := session.Query(q.PreparableQueryString, q.Args...).Iter()
			var x float64
			for iter.Scan(&x) {
				agg = append(agg, x)
			}
			if err := iter.Close(); err != nil {
				return nil, err
			}
		}
		groupResult := qp.Aggregator(agg)
		results = append(results, CQLResult{TimeInterval: k, Value: groupResult})
	}

	return results, nil
}
