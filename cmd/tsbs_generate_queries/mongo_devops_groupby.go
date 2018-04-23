package main

import (
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/query"
)

// MongoDevopsGroupby produces Mongo-specific queries for the devops groupby case.
type MongoDevopsGroupby struct {
	MongoDevops
	numMetrics int
}

// NewMongoDevopsGroupBy produces a function that produces a new MongoDevopsGroupby for the given parameters
func NewMongoDevopsGroupBy(numMetrics int) QueryGeneratorMaker {
	return func(start, end time.Time) QueryGenerator {
		underlying := newMongoDevopsCommon(start, end)
		return &MongoDevopsGroupby{
			MongoDevops: *underlying,
			numMetrics:  numMetrics,
		}
	}
}

// Dispatch fills in the query.Query
func (d *MongoDevopsGroupby) Dispatch(scaleVar int) query.Query {
	q := query.NewMongo() // from pool
	d.GroupByTimeAndPrimaryTag(q, d.numMetrics)
	return q
}
