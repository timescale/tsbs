package main

import "bitbucket.org/440-labs/influxdb-comparisons/query"

// DevopsLastPointPerHost returns QueryFiller for the devops lastpoint case
type DevopsLastPointPerHost struct {
	core DevopsGenerator
}

// NewDevopsLastPointPerHost returns a new DevopsLastPointPerHost for given paremeters
func NewDevopsLastPointPerHost(core DevopsGenerator) QueryFiller {
	return &DevopsLastPointPerHost{core}
}

// Fill fills in the query.Query with query details
func (d *DevopsLastPointPerHost) Fill(q query.Query) query.Query {
	fc, ok := d.core.(LastPointFiller)
	if !ok {
		panicUnimplementedQuery(d.core)
	}
	fc.LastPointPerHost(q)
	return q
}
