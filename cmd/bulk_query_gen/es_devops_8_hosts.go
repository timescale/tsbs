package main

import (
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/query"
)

// ElasticSearchDevops8Hosts produces ElasticSearch-specific queries for the devops groupby case.
type ElasticSearchDevops8Hosts struct {
	ElasticSearchDevops
}

func NewElasticSearchDevops8Hosts(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := NewElasticSearchDevops(dbConfig, start, end).(*ElasticSearchDevops)
	return &ElasticSearchDevops8Hosts{
		ElasticSearchDevops: *underlying,
	}
}

func (d *ElasticSearchDevops8Hosts) Dispatch(_, scaleVar int) query.Query {
	q := NewHTTPQuery() // from pool
	d.MaxCPUUsageHourByMinuteEightHosts(q, scaleVar)
	return q
}
