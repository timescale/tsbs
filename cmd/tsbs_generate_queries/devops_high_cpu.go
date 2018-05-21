package main

import (
	"bitbucket.org/440-labs/influxdb-comparisons/query"
)

// DevopsHighCPU produces a QueryFiller for the devops high-cpu cases
type DevopsHighCPU struct {
	core  DevopsGenerator
	hosts int
}

// NewDevopsHighCPU produces a new function that produces a new DevopsHighCPU
func NewDevopsHighCPU(hosts int) QueryFillerMaker {
	return func(core DevopsGenerator) QueryFiller {
		return &DevopsHighCPU{
			core:  core,
			hosts: hosts,
		}
	}
}

// Fill fills in the query.Query with query details
func (d *DevopsHighCPU) Fill(q query.Query) query.Query {
	fc, ok := d.core.(HighCPUFiller)
	if !ok {
		panicUnimplementedQuery(d.core)
	}
	fc.HighCPUForHosts(q, d.hosts)
	return q
}
