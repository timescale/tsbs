package main

import "time"

// InfluxDevops5Metrics1Host1Hr produces Influx-specific query for the devops 5-metrics, 1 host, 1 hr
type InfluxDevops5Metrics1Host1Hr struct {
	InfluxDevops
}

// NewInfluxDevops5Metrics1Host1Hr creates a new query for the given db and time
func NewInfluxDevops5Metrics1Host1Hr(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newInfluxDevopsCommon(dbConfig, start, end).(*InfluxDevops)
	return &InfluxDevops5Metrics1Host1Hr{
		InfluxDevops: *underlying,
	}
}

func (d *InfluxDevops5Metrics1Host1Hr) Dispatch(_, scaleVar int) Query {
	q := NewHTTPQuery() // from pool
	d.CPU5MetricsHourByMinuteOneHost(q, scaleVar)
	return q
}

// InfluxDevops5Metrics1Host12Hrs produces Influx-specific query for the devops 5-metrics, 1 host, 12 hrs
type InfluxDevops5Metrics1Host12Hrs struct {
	InfluxDevops
}

// NewInfluxDevops5Metrics1Host12Hrs creates a new query for the given db and time
func NewInfluxDevops5Metrics1Host12Hrs(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newInfluxDevopsCommon(dbConfig, start, end).(*InfluxDevops)
	return &InfluxDevops5Metrics1Host12Hrs{
		InfluxDevops: *underlying,
	}
}

func (d *InfluxDevops5Metrics1Host12Hrs) Dispatch(_, scaleVar int) Query {
	q := NewHTTPQuery() // from pool
	d.CPU5Metrics12HoursByMinuteOneHost(q, scaleVar)
	return q
}

// InfluxDevops5Metrics8Hosts1Hr produces Influx-specific query for the devops 5-metrics, 8 hosts, 1 hr
type InfluxDevops5Metrics8Hosts1Hr struct {
	InfluxDevops
}

// NewInfluxDevops5Metrics8Hosts1Hr creates a new query for the given db and time
func NewInfluxDevops5Metrics8Hosts1Hr(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newInfluxDevopsCommon(dbConfig, start, end).(*InfluxDevops)
	return &InfluxDevops5Metrics8Hosts1Hr{
		InfluxDevops: *underlying,
	}
}

func (d *InfluxDevops5Metrics8Hosts1Hr) Dispatch(_, scaleVar int) Query {
	q := NewHTTPQuery() // from pool
	d.CPU5MetricsHourByMinuteEightHosts(q, scaleVar)
	return q
}
