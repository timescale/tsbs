package iot

import (
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/query"
)

// DailyTruckActivity contains info for filling in daily truck activity queries.
type DailyTruckActivity struct {
	core utils.QueryGenerator
}

// NewDailyTruckActivity creates a new daily truck activity query filler.
func NewDailyTruckActivity(core utils.QueryGenerator) utils.QueryFiller {
	return &DailyTruckActivity{
		core: core,
	}
}

// Fill fills in the query.Query with query details.
func (i *DailyTruckActivity) Fill(q query.Query) query.Query {
	fc, ok := i.core.(DailyTruckActivityFiller)
	if !ok {
		common.PanicUnimplementedQuery(i.core)
	}
	fc.DailyTruckActivity(q)
	return q
}
