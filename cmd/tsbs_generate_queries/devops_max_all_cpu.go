package main

import (
	"bitbucket.org/440-labs/influxdb-comparisons/query"
)

// DevopsMaxAllCPU contains info for filling in a query.Query for "max all" queries
type DevopsMaxAllCPU struct {
	core  DevopsGenerator
	hosts int
}

// NewDevopsMaxAllCPU produces a new function that produces a new DevopsAllMaxCPU
func NewDevopsMaxAllCPU(hosts int) QueryFillerMaker {
	return func(core DevopsGenerator) QueryFiller {
		return &DevopsMaxAllCPU{
			core:  core,
			hosts: hosts,
		}
	}
}

// Fill fills in the query.Query with query details
func (d *DevopsMaxAllCPU) Fill(q query.Query) query.Query {
	fc, ok := d.core.(MaxAllFiller)
	if !ok {
		panicUnimplementedQuery(d.core)
	}
	fc.MaxAllCPU(q, d.hosts)
	return q
}
