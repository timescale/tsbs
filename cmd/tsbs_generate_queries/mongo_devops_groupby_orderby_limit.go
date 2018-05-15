package main

import (
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/query"
)

// MongoDevopsGroupByOrderByLimit produces Mongo-specific queries for the devops groupby-orderby-limit case.
type MongoDevopsGroupByOrderByLimit struct {
	MongoDevops
}

// NewMongoDevopsGroupByOrderByLimit returns a new MongoDevopsGroupByOrderByLimit for given paremeters
func NewMongoDevopsGroupByOrderByLimit(start, end time.Time, scale int) QueryGenerator {
	underlying := newMongoDevopsCommon(start, end, scale)
	return &MongoDevopsGroupByOrderByLimit{
		MongoDevops: *underlying,
	}
}

// Dispatch fills in the query.Query
func (d *MongoDevopsGroupByOrderByLimit) Dispatch() query.Query {
	q := query.NewMongo() // from pool
	d.GroupByOrderByLimit(q)
	return q
}
