package main

import (
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/query"
)

// OpenTSDBDevops8Hosts produces OpenTSDB-specific queries for the devops groupby case.
type OpenTSDBDevops8Hosts struct {
	OpenTSDBDevops
}

func NewOpenTSDBDevops8Hosts(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newOpenTSDBDevopsCommon(dbConfig, start, end).(*OpenTSDBDevops)
	return &OpenTSDBDevops8Hosts{
		OpenTSDBDevops: *underlying,
	}
}

func (d *OpenTSDBDevops8Hosts) Dispatch(_, scaleVar int) query.Query {
	q := NewHTTPQuery() // from pool
	d.MaxCPUUsageHourByMinuteEightHosts(q, scaleVar)
	return q
}
