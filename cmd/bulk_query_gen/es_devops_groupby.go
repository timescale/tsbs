package main

import (
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/query"
)

// ElasticSearchDevopsGroupBy produces ES-specific queries for the devops groupby case.
type ElasticSearchDevopsGroupBy struct {
	ElasticSearchDevops
}

func NewElasticSearchDevopsGroupBy(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := NewElasticSearchDevops(dbConfig, start, end).(*ElasticSearchDevops)
	return &ElasticSearchDevopsGroupBy{
		ElasticSearchDevops: *underlying,
	}
}

func (d *ElasticSearchDevopsGroupBy) Dispatch(i, scaleVar int) query.Query {
	q := NewHTTPQuery() // from pool
	d.MeanCPUUsageDayByHourAllHostsGroupbyHost(q, scaleVar)
	return q
}
