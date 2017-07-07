package main

import (
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/query"
)

// ElasticSearchDevopsSingleHost12hr produces ES-specific queries for the devops single-host case.
type ElasticSearchDevopsSingleHost12hr struct {
	ElasticSearchDevops
}

func NewElasticSearchDevopsSingleHost12hr(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := NewElasticSearchDevops(dbConfig, start, end).(*ElasticSearchDevops)
	return &ElasticSearchDevopsSingleHost12hr{
		ElasticSearchDevops: *underlying,
	}
}

func (d *ElasticSearchDevopsSingleHost12hr) Dispatch(i, scaleVar int) query.Query {
	q := NewHTTPQuery() // from pool
	d.MaxCPUUsage12HoursByMinuteOneHost(q, scaleVar)
	return q
}
