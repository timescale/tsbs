package main

import (
	"fmt"

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
	tis := []TimeInterval{}
  	for ti, cqlGroup := range qp.BucketedCQLQueries {
		tis = append(tis, ti)
		//fmt.Printf("[qp_exec] %s\n", ti)
		_ = cqlGroup
  	}

	min := tis[0]
	max := tis[0]

	for _, ti := range tis[1:] {
		if ti.Start.Before(min.Start) {
			min = ti
		}
		if ti.Start.After(max.Start) {
			max = ti
		}
	}

	fmt.Printf("[exec] %s - %s\n", min, max)

	results := make([]float64, 0, len(qp.BucketedCQLQueries))
  	for _, cqlGroup := range qp.BucketedCQLQueries {
		agg := make([]float64, 0, len(cqlGroup))
		for _, query := range cqlGroup {
			fmt.Println(string(query))
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
