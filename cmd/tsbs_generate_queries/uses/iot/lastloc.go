package iot

import (
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/query"
)

// LastLocPerTruck contains info for filling in last location queries.
type LastLocPerTruck struct {
	core utils.QueryGenerator
}

// NewLastLocPerTruck creates a new last location query filler.
func NewLastLocPerTruck(core utils.QueryGenerator) utils.QueryFiller {
	return &LastLocPerTruck{
		core: core,
	}
}

// Fill fills in the query.Query with query details
func (i *LastLocPerTruck) Fill(q query.Query) query.Query {
	fc, ok := i.core.(LastLocFiller)
	if !ok {
		common.PanicUnimplementedQuery(i.core)
	}
	fc.LastLocPerTruck(q)
	return q
}
