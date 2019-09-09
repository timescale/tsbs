package iot

import (
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/query"
)

// TrucksWithLongDailySession contains info for filling in trucks with longer driving session queries.
type TrucksWithLongDailySession struct {
	core utils.QueryGenerator
}

// NewTruckWithLongDailySession creates a new trucks with longer driving session query filler.
func NewTruckWithLongDailySession(core utils.QueryGenerator) utils.QueryFiller {
	return &TrucksWithLongDailySession{
		core: core,
	}
}

// Fill fills in the query.Query with query details.
func (i *TrucksWithLongDailySession) Fill(q query.Query) query.Query {
	fc, ok := i.core.(TruckLongDailySessionFiller)
	if !ok {
		common.PanicUnimplementedQuery(i.core)
	}
	fc.TrucksWithLongDailySessions(q)
	return q
}
