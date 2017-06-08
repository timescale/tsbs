package main

import (
	"bitbucket.org/440-labs/influxdb-comparisons/benchmarker"
)

// CStat represents one statistical measurement for Cassandra, which
// includes an extra field to determine which stat is the actual, full
// measurement.
type CStat struct {
	benchmarker.Stat
	IsActual bool
}

// Init safely initializes a stat while minimizing heap allocations.
func (c *CStat) Init(label []byte, value float64, isActual bool) {
	c.Stat.Init(label, value)
	c.IsActual = isActual
}
