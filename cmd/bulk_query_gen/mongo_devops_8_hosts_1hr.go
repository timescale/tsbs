package main

import "time"

// MongoDevopsSingleHost produces Mongo-specific queries for the devops single-host case.
type MongoDevops8Hosts1Hr struct {
	MongoDevops
}

func NewMongoDevops8Hosts1Hr(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := NewMongoDevops(dbConfig, start, end).(*MongoDevops)
	return &MongoDevops8Hosts1Hr{
		MongoDevops: *underlying,
	}
}

func (d *MongoDevops8Hosts1Hr) Dispatch(i, scaleVar int) Query {
	q := NewMongoQuery() // from pool
	d.MaxCPUUsageHourByMinuteEightHosts(q, scaleVar)
	return q
}
