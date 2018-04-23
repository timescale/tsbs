package main

import (
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/query"
)

// MongoNaiveDevopsSingleMetric contains info for Mongo-devops test '*-hosts-*-hrs'
type MongoNaiveDevopsSingleMetric struct {
	MongoDevops
	hosts int
	hours int
}

// NewMongoNaiveDevopsSingleMetric produces a new function that produces a new MongoNaiveDevopsSingleMetric
func NewMongoNaiveDevopsSingleMetric(hosts, hours int) QueryGeneratorMaker {
	return func(start, end time.Time) QueryGenerator {
		underlying := newMongoDevopsCommon(start, end)
		return &MongoNaiveDevopsSingleMetric{
			MongoDevops: *underlying,
			hosts:       hosts,
			hours:       hours,
		}
	}
}

// Dispatch fills in the query.Query
func (d *MongoNaiveDevopsSingleMetric) Dispatch(scaleVar int) query.Query {
	q := query.NewMongo() // from pool
	d.MaxCPUUsageHourByMinuteNaive(q, scaleVar, d.hosts, time.Duration(int64(d.hours)*int64(time.Hour)))
	return q
}
