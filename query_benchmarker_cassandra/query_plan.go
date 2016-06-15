package main

import (
	"fmt"
	"sort"

	"github.com/gocql/gocql"
)

// A QueryPlan wraps the literal CQL query data needed to fulfill an HLQuery.
type QueryPlan struct {
	Aggregator     AggrFunc
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
		Aggregator:     aggr,
		BucketedCQLQueries: bucketedCQLQueries,
	}
	return qp, nil
}

// Execute runs all CQLQueries in the QueryPlan, possibly in parallel.
func (qp *QueryPlan) Execute(session *gocql.Session) error {
	// sort the time interval buckets we'll use:
	sortedKeys := make([]TimeInterval, 0, len(qp.BucketedCQLQueries))
	for k := range qp.BucketedCQLQueries {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Sort(TimeIntervals(sortedKeys))

	// for each bucket, execute its queries, aggregate the results, then
	// store them:
	results := make([]float64, 0, len(qp.BucketedCQLQueries))
	for _, k := range sortedKeys {
		cqlGroup := qp.BucketedCQLQueries[k]
		agg := make([]float64, 0, len(cqlGroup))
		for _, query := range cqlGroup {
			//fmt.Println(string(query))
			iter := session.Query(string(query)).Iter()
			var x float64
			for iter.Scan(&x) {
				agg = append(agg, x)
			}
			if err := iter.Close(); err != nil {
				return err
			}
		}
		groupResult := qp.Aggregator(agg)
		results = append(results, groupResult)
	}

	fmt.Printf("[exec] %v...\n", results[:8])
	return nil
}
