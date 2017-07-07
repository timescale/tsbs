package main

import (
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/query"
)

// MongoDevopsSingleHost produces Mongo-specific queries for the devops single-host case.
type MongoDevopsSingleHost struct {
	MongoDevops
}

func NewMongoDevopsSingleHost(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := NewMongoDevops(dbConfig, start, end).(*MongoDevops)
	return &MongoDevopsSingleHost{
		MongoDevops: *underlying,
	}
}

func (d *MongoDevopsSingleHost) Dispatch(i, scaleVar int) query.Query {
	q := NewMongoQuery() // from pool
	d.MaxCPUUsageHourByMinuteOneHost(q, scaleVar)
	return q
}
