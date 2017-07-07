package main

import (
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/query"
)

// OpenTSDBDevopsSingleHost produces OpenTSDB-specific queries for the devops single-host case.
type OpenTSDBDevopsSingleHost struct {
	OpenTSDBDevops
}

func NewOpenTSDBDevopsSingleHost(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newOpenTSDBDevopsCommon(dbConfig, start, end).(*OpenTSDBDevops)
	return &OpenTSDBDevopsSingleHost{
		OpenTSDBDevops: *underlying,
	}
}

func (d *OpenTSDBDevopsSingleHost) Dispatch(i, scaleVar int) query.Query {
	q := NewHTTPQuery() // from pool
	d.MaxCPUUsageHourByMinuteOneHost(q, scaleVar)
	return q
}
