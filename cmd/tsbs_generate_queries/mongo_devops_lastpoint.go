package main

import (
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/query"
)

// MongoDevopsLastPointPerHost produces Mongo-specific queries for the devops lastpoint case
type MongoDevopsLastPointPerHost struct {
	MongoDevops
}

// NewMongoDevopsLastPointPerHost returns a new MongoDevopsLastPointPerHost for given paremeters
func NewMongoDevopsLastPointPerHost(start, end time.Time, scale int) QueryGenerator {
	underlying := newMongoDevopsCommon(start, end, scale)
	return &MongoDevopsLastPointPerHost{
		MongoDevops: *underlying,
	}

}

// Dispatch fills in the query.Query
func (d *MongoDevopsLastPointPerHost) Dispatch() query.Query {
	q := query.NewMongo() // from pool
	d.LastPointPerHost(q)
	return q
}
