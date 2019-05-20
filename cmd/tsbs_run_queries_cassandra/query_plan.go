package main

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"github.com/timescale/tsbs/internal/utils"
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
	BucketedCQLQueries map[*utils.TimeInterval][]CQLQuery
}

// NewQueryPlanWithServerAggregation builds a QueryPlanWithServerAggregation.
// It is typically called via (*HLQuery).ToQueryPlanWithServerAggregation.
func NewQueryPlanWithServerAggregation(aggrLabel string, bucketedCQLQueries map[*utils.TimeInterval][]CQLQuery) (*QueryPlanWithServerAggregation, error) {
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
	sortedKeys := make([]*utils.TimeInterval, 0, len(qp.BucketedCQLQueries))
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
				fmt.Printf("[qpsa] CQL: %v, %d, %s\n", k, i, q)
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
	Aggregators     map[*utils.TimeInterval]map[string]Aggregator
	GroupByDuration time.Duration
	Fields          []string
	TimeBuckets     []*utils.TimeInterval
	limit           int
	CQLQueries      []CQLQuery
}

// NewQueryPlanWithoutServerAggregation builds a QueryPlanWithoutServerAggregation.
// It is typically called via (*HLQuery).ToQueryPlanWithoutServerAggregation.
func NewQueryPlanWithoutServerAggregation(aggrLabel string, groupByDuration time.Duration, fields []string, timeBuckets []*utils.TimeInterval, limit int, cqlQueries []CQLQuery) (*QueryPlanWithoutServerAggregation, error) {
	aggrs := make(map[*utils.TimeInterval]map[string]Aggregator, len(timeBuckets))
	for _, ti := range timeBuckets {
		if len(aggrs) > 0 && len(aggrs) == limit {
			break
		}
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
		limit:           limit,
		CQLQueries:      cqlQueries,
	}
	return qp, nil
}

func csiDebugQueries(cqlQueries []CQLQuery, label string, level int) {
	if level >= 1 {
		fmt.Printf("[%s] query with client aggregation plan has %d CQLQuery objects\n", label, len(cqlQueries))
	}

	if level >= 2 {
		for i, q := range cqlQueries {
			fmt.Printf("[%s] CQL: %d, %s\n", label, i, q)
		}
	}
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
			bucketKey, err := utils.NewTimeInterval(tsTruncated, tsTruncated.Add(qp.GroupByDuration))
			if err != nil {
				return nil, err
			}

			// Due to limits, bucket is not needed, skip
			if _, ok := qp.Aggregators[bucketKey]; !ok {
				break
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
		if _, ok := qp.Aggregators[ti]; !ok {
			continue
		}

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
	csiDebugQueries(qp.CQLQueries, "qpca", level)
}

// QueryPlanNoAggregation fulfills an HLQuery by performing queries on the
// server and combining columns into a row on the client when there is no aggregator.
type QueryPlanNoAggregation struct {
	fields     []string
	where      string
	cqlQueries []CQLQuery
}

// NewQueryPlanNoAggregation builds a QueryPlanWithoutServerAggregation.
// It is typically called via (*HLQuery).ToQueryPlanWithoutServerAggregation.
func NewQueryPlanNoAggregation(fields []string, where string, cqlQueries []CQLQuery) (*QueryPlanNoAggregation, error) {
	return &QueryPlanNoAggregation{
		fields:     fields,
		cqlQueries: cqlQueries,
		where:      where,
	}, nil
}

type int64arr []int64

func (a int64arr) Len() int           { return len(a) }
func (a int64arr) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a int64arr) Less(i, j int) bool { return a[i] < a[j] }

// Execute runs all CQLQueries in the QueryPlan and collects the results.
//
// TODO(rw): support parallel execution.
func (qp *QueryPlanNoAggregation) Execute(session *gocql.Session) ([]CQLResult, error) {
	res := make(map[int64]map[string][]float64)
	// Useful index for placing values in a row correctly
	fieldPos := make(map[string]int)
	for i, f := range qp.fields {
		fieldPos[f] = i
	}

	whereParts := strings.Split(qp.where, ",")

	// If a where clause exists, cycle through queries finding the ones
	// that are queries on the field in the where clause.
	//
	// First execute those to find the set of (timestamp, series) that
	// match the where cluase. Then, execute the rest of the queries
	// while checking that their timestamp, series match.
	//
	// This approach allows us to discard rows that don't match instead
	// of pulling all rows and then filtering afterwards.
	if len(whereParts) == 3 {
		whereFn := getWhereFn(whereParts[1], whereParts[2])

		// First pass of all queries
		for _, q := range qp.cqlQueries {
			if q.Field == whereParts[0] { // only handle queries for where clause field
				iter := session.Query(q.PreparableQueryString, q.Args...).Iter()

				var timestampNs int64
				var value float64

				key := strings.Replace(q.Args[0].(string), q.Field, "", 1)
				for iter.Scan(&timestampNs, &value) {
					// Skip rows that do not match where clause
					if !whereFn(value) {
						continue
					}

					if _, ok := res[timestampNs]; !ok {
						res[timestampNs] = make(map[string][]float64)
					}
					if _, ok := res[timestampNs][key]; !ok {
						res[timestampNs][key] = make([]float64, len(qp.fields))
					}
					res[timestampNs][key][fieldPos[q.Field]] = value
				}
				if err := iter.Close(); err != nil {
					return nil, err
				}
			}
		}

		// Second pass for non-where clause fields
		for _, q := range qp.cqlQueries {
			if q.Field != whereParts[0] {
				iter := session.Query(q.PreparableQueryString, q.Args...).Iter()

				var timestampNs int64
				var value float64

				key := strings.Replace(q.Args[0].(string), q.Field, "", 1)
				for iter.Scan(&timestampNs, &value) {
					// First pass added the only timestamps or series we accept
					if _, ok := res[timestampNs]; !ok {
						continue
					}
					if _, ok := res[timestampNs][key]; !ok {
						continue
					}
					res[timestampNs][key][fieldPos[q.Field]] = value
				}
				if err := iter.Close(); err != nil {
					return nil, err
				}
			}
		}
	} else {
		// TODO support no where clause?
	}

	keys := make(int64arr, len(res))
	i := 0
	for k := range res {
		keys[i] = k
		i++
	}
	sort.Sort(keys)

	results := make([]CQLResult, 0, len(res))
	for _, ts := range keys {
		tst := time.Unix(0, ts)
		for _, vals := range res[ts] {
			ti, err := utils.NewTimeInterval(tst, tst)
			if err != nil {
				return nil, err
			}
			temp := CQLResult{
				TimeInterval: ti,
				Values:       vals,
			}
			results = append(results, temp)
		}
	}

	return results, nil
}

// DebugQueries prints debugging information.
func (qp *QueryPlanNoAggregation) DebugQueries(level int) {
	csiDebugQueries(qp.cqlQueries, "qpna", level)
}

// getWhereFn creates a function to evaluate a single WHERE clause true or false
func getWhereFn(op, pred string) func(float64) bool {
	p, err := strconv.ParseFloat(pred, 64)
	if err != nil {
		panic("unknown predicate in where clause: " + pred)
	}
	if op == ">" {
		return func(x float64) bool { return x > p }
	} else if op == "<" {
		return func(x float64) bool { return x < p }
	} else if op == ">=" {
		return func(x float64) bool { return x >= p }
	} else if op == "<=" {
		return func(x float64) bool { return x <= p }
	}
	panic("unsupported operator in where clause: " + op)
}

// QueryPlanForEvery fulfills an HLQuery by performing queries on the
// server and combining columns into a row on the client to create
// fulfill an N-for-every query.
//
// An N-for-every query retries the last N (TODO - first N) for every
// particular tag. For example, the last 1 row for every host.
type QueryPlanForEvery struct {
	fields      []string
	forEveryTag string
	forEveryNum int64
	cqlQueries  []CQLQuery
}

// NewQueryPlanForEvery builds a QueryPlanForEvery.
func NewQueryPlanForEvery(fields []string, forEveryTag string, forEveryNum int64, cqlQueries []CQLQuery) (*QueryPlanForEvery, error) {
	return &QueryPlanForEvery{
		fields:      fields,
		forEveryTag: forEveryTag,
		forEveryNum: forEveryNum,
		cqlQueries:  cqlQueries,
	}, nil
}

// Execute runs all CQLQueries in the QueryPlan and collects the results.
//
// TODO(rw): support parallel execution.
func (qp *QueryPlanForEvery) Execute(session *gocql.Session) ([]CQLResult, error) {
	res := make(map[string]map[int64][]float64)
	seriesTracker := make(map[string]int)

	// Use a regex to match a particular tag
	r, err := regexp.Compile(qp.forEveryTag + "=(.+?),")
	if err != nil {
		panic("could not compile regex for tag: " + qp.forEveryTag)
	}

	for _, q := range qp.cqlQueries {
		iter := session.Query(q.PreparableQueryString, q.Args...).Iter()

		rm := r.FindSubmatch([]byte(q.Args[0].(string)))
		key := string(rm[1])

		// Only run the query if this forEveryTag has not been filled
		// up yet. Once we have all the values for this value, no need
		// to run other queries that have the same value.
		// TODO - Generalize for N instead of 1
		if _, ok := res[key]; !ok {
			res[key] = make(map[int64][]float64)
			seriesTracker[key] = 0
		} else if seriesTracker[key] == len(qp.fields) {
			// Collected values for each field in the row, no need to
			// do more queries
			continue
		}

		var timestampNs int64
		var value float64
		for iter.Scan(&timestampNs, &value) {
			// Haven't encountered this host yet
			// TODO - for N, need to keep making timestamp secondary keys until N
			if len(res[key]) == 0 {
				res[key][timestampNs] = make([]float64, 0)
			}

			if _, ok := res[key][timestampNs]; !ok {
				// Sorted by descending, so once we encounter one not in our
				// map, we can skip. It will be added to the map in previous step.
				break
			}

			// TODO put in proper position according to field
			res[key][timestampNs] = append(res[key][timestampNs], value)
			seriesTracker[key]++
			if seriesTracker[key] == len(qp.fields) {
				break
			}
		}
		if err := iter.Close(); err != nil {
			return nil, err
		}
	}

	results := make([]CQLResult, 0, len(res))
	// TODO should print out each host
	for _, map2 := range res {
		for ts, vals := range map2 {
			tst := time.Unix(0, ts)
			ti, err := utils.NewTimeInterval(tst, tst)
			if err != nil {
				return nil, err
			}
			temp := CQLResult{
				TimeInterval: ti,
				Values:       vals,
			}
			results = append(results, temp)
		}
	}

	return results, nil
}

// DebugQueries prints debugging information.
func (qp *QueryPlanForEvery) DebugQueries(level int) {
	csiDebugQueries(qp.cqlQueries, "qpfe", level)
}
