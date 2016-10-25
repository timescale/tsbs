package main

import "time"

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

func (d *OpenTSDBDevopsSingleHost12hr) Dispatch(i, scaleVar int) Query {
	q := NewHTTPQuery() // from pool
	d.MaxCPUUsage12HoursByMinuteOneHost(q, scaleVar)
	return q
}

