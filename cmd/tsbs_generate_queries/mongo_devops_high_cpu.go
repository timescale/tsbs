package main

import (
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/query"
)

// MongoDevopsHighCPU produces Mongo-specific queries for the devops high-cpu cases
type MongoDevopsHighCPU struct {
	MongoDevops
	hosts int
}

// NewMongoDevopsHighCPU produces a new function that produces a new MongoDevopsHighCPU
func NewMongoDevopsHighCPU(hosts int) QueryGeneratorMaker {
	return func(start, end time.Time, scale int) QueryGenerator {
		underlying := newMongoDevopsCommon(start, end, scale)
		return &MongoDevopsHighCPU{
			MongoDevops: *underlying,
			hosts:       hosts,
		}
	}
}

// Dispatch fills in the query.Query
func (d *MongoDevopsHighCPU) Dispatch() query.Query {
	q := query.NewMongo() // from pool
	d.HighCPUForHosts(q, d.hosts)
	return q
}
