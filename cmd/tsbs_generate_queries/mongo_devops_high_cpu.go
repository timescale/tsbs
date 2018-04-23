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
	return func(start, end time.Time) QueryGenerator {
		underlying := newMongoDevopsCommon(start, end)
		return &MongoDevopsHighCPU{
			MongoDevops: *underlying,
			hosts:       hosts,
		}
	}
}

// Dispatch fills in the query.Query
func (d *MongoDevopsHighCPU) Dispatch(scaleVar int) query.Query {
	q := query.NewMongo() // from pool
	d.HighCPUForHosts(q, scaleVar, d.hosts)
	return q
}
