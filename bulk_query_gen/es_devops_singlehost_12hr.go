package main

import "time"

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

func (d *ElasticSearchDevopsSingleHost12hr) Dispatch(i, scaleVar int) Query {
	q := NewHTTPQuery() // from pool
	d.MaxCPUUsage12HoursByMinuteOneHost(q, scaleVar)
	return q
}
