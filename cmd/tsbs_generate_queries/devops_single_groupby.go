package main

import (
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/query"
)

// DevopsSingleGroupby contains info for filling in single groupby queries
type DevopsSingleGroupby struct {
	core    DevopsGenerator
	metrics int
	hosts   int
	hours   int
}

// NewDevopsSingleGroupby produces a new function that produces a new DevopsSingleGroupby
func NewDevopsSingleGroupby(metrics, hosts, hours int) QueryFillerMaker {
	return func(core DevopsGenerator) QueryFiller {
		return &DevopsSingleGroupby{
			core:    core,
			metrics: metrics,
			hosts:   hosts,
			hours:   hours,
		}
	}
}

// Fill fills in the query.Query with query details
func (d *DevopsSingleGroupby) Fill(q query.Query) query.Query {
	fc, ok := d.core.(SingleGroupbyFiller)
	if !ok {
		panicUnimplementedQuery(d.core)
	}
	fc.GroupByTime(q, d.hosts, d.metrics, time.Duration(int64(d.hours)*int64(time.Hour)))
	return q
}
