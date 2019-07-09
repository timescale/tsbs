package iot

import (
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/query"
)

// TrucksWithLongDrivingSession contains info for filling in trucks with longer driving sessions queries.
type TrucksWithLongDrivingSession struct {
	core utils.QueryGenerator
}

// NewTrucksWithLongDrivingSession creates a new trucks with longer driving sessions query filler.
func NewTrucksWithLongDrivingSession(core utils.QueryGenerator) utils.QueryFiller {
	return &TrucksWithLongDrivingSession{
		core: core,
	}
}

// Fill fills in the query.Query with query details.
func (i *TrucksWithLongDrivingSession) Fill(q query.Query) query.Query {
	fc, ok := i.core.(TruckLongDrivingSessionFiller)
	if !ok {
		common.PanicUnimplementedQuery(i.core)
	}
	fc.TrucksWithLongDrivingSessions(q)
	return q
}
