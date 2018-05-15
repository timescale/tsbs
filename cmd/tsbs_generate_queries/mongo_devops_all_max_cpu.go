package main

import (
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/query"
)

// MongoDevopsAllMaxCPU contains info for Mongo-devops test 'cpu-max-all-*'
type MongoDevopsAllMaxCPU struct {
	MongoDevops
	hosts int
}

// NewMongoDevopsAllMaxCPU produces a new function that produces a new MongoDevopsAllMaxCPU
func NewMongoDevopsAllMaxCPU(hosts int) QueryGeneratorMaker {
	return func(start, end time.Time, scale int) QueryGenerator {
		underlying := newMongoDevopsCommon(start, end, scale)
		return &MongoDevopsAllMaxCPU{
			MongoDevops: *underlying,
			hosts:       hosts,
		}
	}
}

// Dispatch fills in the query.Query
func (d *MongoDevopsAllMaxCPU) Dispatch() query.Query {
	q := query.NewMongo() // from pool
	d.MaxAllCPU(q, d.hosts)
	return q
}
