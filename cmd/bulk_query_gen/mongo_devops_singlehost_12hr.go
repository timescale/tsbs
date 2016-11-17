package main

import "time"

// MongoDevopsSingleHost produces Mongo-specific queries for the devops single-host case.
type MongoDevopsSingleHost12hr struct {
	MongoDevops
}

func NewMongoDevopsSingleHost12hr(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := NewMongoDevops(dbConfig, start, end).(*MongoDevops)
	return &MongoDevopsSingleHost12hr{
		MongoDevops: *underlying,
	}
}

func (d *MongoDevopsSingleHost12hr) Dispatch(i, scaleVar int) Query {
	q := NewMongoQuery() // from pool
	d.MaxCPUUsage12HoursByMinuteOneHost(q, scaleVar)
	return q
}
