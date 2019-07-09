package iot

import (
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/query"
)

// AvgDailyDrivingDuration contains info for filling in avg daily driving duration per driver queries.
type AvgDailyDrivingDuration struct {
	core utils.QueryGenerator
}

// NewAvgDailyDrivingDuration creates a new avg daily driving duration per driver query filler.
func NewAvgDailyDrivingDuration(core utils.QueryGenerator) utils.QueryFiller {
	return &AvgDailyDrivingDuration{
		core: core,
	}
}

// Fill fills in the query.Query with query details.
func (i *AvgDailyDrivingDuration) Fill(q query.Query) query.Query {
	fc, ok := i.core.(AvgDailyDrivingDurationFiller)
	if !ok {
		common.PanicUnimplementedQuery(i.core)
	}
	fc.AvgDailyDrivingDuration(q)
	return q
}
