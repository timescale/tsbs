package iot

import (
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/query"
)

// AvgDailyDrivingSession contains info for filling in avg daily driving session queries.
type AvgDailyDrivingSession struct {
	core utils.QueryGenerator
}

// NewAvgDailyDrivingSession creates a new avg daily driving session query filler.
func NewAvgDailyDrivingSession(core utils.QueryGenerator) utils.QueryFiller {
	return &AvgDailyDrivingSession{
		core: core,
	}
}

// Fill fills in the query.Query with query details.
func (i *AvgDailyDrivingSession) Fill(q query.Query) query.Query {
	fc, ok := i.core.(AvgDailyDrivingSessionFiller)
	if !ok {
		common.PanicUnimplementedQuery(i.core)
	}
	fc.AvgDailyDrivingSession(q)
	return q
}
