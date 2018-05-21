package main

import (
	"bitbucket.org/440-labs/influxdb-comparisons/query"
)

// DevopsGroupByOrderByLimit produces a filler for queries in the devops groupby-orderby-limit case.
type DevopsGroupByOrderByLimit struct {
	core DevopsGenerator
}

// NewDevopsGroupByOrderByLimit returns a new DevopsGroupByOrderByLimit for given paremeters
func NewDevopsGroupByOrderByLimit(core DevopsGenerator) QueryFiller {
	return &DevopsGroupByOrderByLimit{core}
}

// Fill fills in the query.Query with query details
func (d *DevopsGroupByOrderByLimit) Fill(q query.Query) query.Query {
	fc, ok := d.core.(GroupbyOrderbyLimitFiller)
	if !ok {
		panicUnimplementedQuery(d.core)
	}
	fc.GroupByOrderByLimit(q)
	return q
}
