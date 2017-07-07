package main

import (
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/query"
)

// OpenTSDBDevopsSingleHost12hr produces OpenTSDB-specific queries for the devops single-host case over a 12hr period.
type OpenTSDBDevopsSingleHost12hr struct {
	OpenTSDBDevops
}

func NewOpenTSDBDevopsSingleHost12hr(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newOpenTSDBDevopsCommon(dbConfig, start, end).(*OpenTSDBDevops)
	return &OpenTSDBDevopsSingleHost12hr{
		OpenTSDBDevops: *underlying,
	}
}

func (d *OpenTSDBDevopsSingleHost12hr) Dispatch(i, scaleVar int) query.Query {
	q := NewHTTPQuery() // from pool
	d.MaxCPUUsage12HoursByMinuteOneHost(q, scaleVar)
	return q
}
