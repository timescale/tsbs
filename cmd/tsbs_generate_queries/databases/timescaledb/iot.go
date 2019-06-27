package timescaledb

import (
	"fmt"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/iot"
	"github.com/timescale/tsbs/query"
)

const (
	iotReadingsTable = "readings"
)

// IoT produces TimescaleDB-specific queries for all the iot query types.
type IoT struct {
	*iot.Core
	*BaseGenerator
}

// NewIoT makes an IoT object ready to generate Queries.
func NewIoT(start, end time.Time, scale int, g *BaseGenerator) *IoT {
	c, err := iot.NewCore(start, end, scale)
	panicIfErr(err)
	return &IoT{
		Core:          c,
		BaseGenerator: g,
	}
}

// LastLocPerTruck finds all the truck locations along with fleet and driver names.
func (i *IoT) LastLocPerTruck(qi query.Query) {
	sql := fmt.Sprintf("SELECT t.fleet, t.driver, r.* FROM tags t INNER JOIN LATERAL "+
		"(SELECT longitude, latitude FROM readings r WHERE r.tags_id=t.id ORDER BY time DESC LIMIT 1) "+
		"r ON true WHERE t.fleet = '%s'", i.GetRandomFleet())
	humanLabel := "TimescaleDB last location per truck"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, iot.ReadingsTableName, sql)
}
