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
	return func(start, end time.Time) QueryGenerator {
		underlying := newMongoDevopsCommon(start, end)
		return &MongoDevopsAllMaxCPU{
			MongoDevops: *underlying,
			hosts:       hosts,
		}
	}
}

// Dispatch fills in the query.Query
func (d *MongoDevopsAllMaxCPU) Dispatch(scaleVar int) query.Query {
	q := query.NewMongo() // from pool
	d.MaxAllCPU(q, scaleVar, d.hosts)
	return q
}
