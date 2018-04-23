package main

import (
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/query"
)

// MongoDevopsSingleMetric contains info for Mongo-devops test '*-hosts-*-hrs'
type MongoDevopsSingleMetric struct {
	MongoDevops
	hosts int
	hours int
}

// NewMongoDevopsSingleMetric produces a new function that produces a new MongoDevopsSingleMetric
func NewMongoDevopsSingleMetric(hosts, hours int) QueryGeneratorMaker {
	return func(start, end time.Time) QueryGenerator {
		underlying := newMongoDevopsCommon(start, end)
		return &MongoDevopsSingleMetric{
			MongoDevops: *underlying,
			hosts:       hosts,
			hours:       hours,
		}
	}
}

// Dispatch fills in the query.Query
func (d *MongoDevopsSingleMetric) Dispatch(scaleVar int) query.Query {
	q := query.NewMongo() // from pool
	d.MaxCPUUsageHourByMinute(q, scaleVar, d.hosts, 1, time.Duration(int64(d.hours)*int64(time.Hour)))
	return q
}
