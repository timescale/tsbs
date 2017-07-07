package main

import (
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/query"
)

// ElasticSearchDevopsSingleHost produces ES-specific queries for the devops single-host case.
type ElasticSearchDevopsSingleHost struct {
	ElasticSearchDevops
}

func NewElasticSearchDevopsSingleHost(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := NewElasticSearchDevops(dbConfig, start, end).(*ElasticSearchDevops)
	return &ElasticSearchDevopsSingleHost{
		ElasticSearchDevops: *underlying,
	}
}

func (d *ElasticSearchDevopsSingleHost) Dispatch(i, scaleVar int) query.Query {
	q := NewHTTPQuery() // from pool
	d.MaxCPUUsageHourByMinuteOneHost(q, scaleVar)
	return q
}
