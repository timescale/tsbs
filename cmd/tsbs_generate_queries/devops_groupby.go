package main

import (
	"bitbucket.org/440-labs/influxdb-comparisons/query"
)

// DevopsGroupby produces a QueryFiller for the devops groupby case.
type DevopsGroupby struct {
	core       DevopsGenerator
	numMetrics int
}

// NewDevopsGroupBy produces a function that produces a new DevopsGroupby for the given parameters
func NewDevopsGroupBy(numMetrics int) QueryFillerMaker {
	return func(core DevopsGenerator) QueryFiller {
		return &DevopsGroupby{
			core:       core,
			numMetrics: numMetrics,
		}
	}
}

// Fill fills in the query.Query with query details
func (d *DevopsGroupby) Fill(q query.Query) query.Query {
	fc, ok := d.core.(DoubleGroupbyFiller)
	if !ok {
		panicUnimplementedQuery(d.core)
	}
	fc.GroupByTimeAndPrimaryTag(q, d.numMetrics)
	return q
}
