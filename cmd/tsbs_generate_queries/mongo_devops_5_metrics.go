package main

import (
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/query"
)

// MongoDevops5Metrics contains info for Mongo-devops test '5-metrics-*'
type MongoDevops5Metrics struct {
	MongoDevops
	hosts int
	hours int
}

// NewMongoDevops5Metrics produces a new function that produces a new MongoDevops5Metrics
func NewMongoDevops5Metrics(hosts, hours int) QueryGeneratorMaker {
	return func(start, end time.Time) QueryGenerator {
		underlying := newMongoDevopsCommon(start, end)
		return &MongoDevops5Metrics{
			MongoDevops: *underlying,
			hosts:       hosts,
			hours:       hours,
		}
	}
}

// Dispatch fills in the query.Query
func (d *MongoDevops5Metrics) Dispatch(scaleVar int) query.Query {
	q := query.NewMongo() // from pool
	d.MaxCPUUsageHourByMinute(q, scaleVar, d.hosts, 5, time.Duration(int64(d.hours)*int64(time.Hour)))
	return q
}
