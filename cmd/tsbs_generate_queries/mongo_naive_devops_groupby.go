package main

import (
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/query"
)

// MongoNaiveDevopsGroupby produces Mongo-specific queries for the devops groupby case.
type MongoNaiveDevopsGroupby struct {
	MongoDevops
	numMetrics int
}

// NewMongoNaiveDevopsGroupBy produces a function that produces a new MongoNaiveDevopsGroupby for the given parameters
func NewMongoNaiveDevopsGroupBy(numMetrics int) QueryGeneratorMaker {
	return func(start, end time.Time, scale int) QueryGenerator {
		underlying := newMongoDevopsCommon(start, end, scale)
		return &MongoNaiveDevopsGroupby{
			MongoDevops: *underlying,
			numMetrics:  numMetrics,
		}
	}
}

// Dispatch fills in the query.Query
func (d *MongoNaiveDevopsGroupby) Dispatch() query.Query {
	q := query.NewMongo() // from pool
	d.GroupByTimeAndPrimaryTagNaive(q, d.numMetrics)
	return q
}
