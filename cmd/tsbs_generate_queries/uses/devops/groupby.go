package devops

import (
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
)

// Groupby produces a QueryFiller for the devops groupby case.
type Groupby struct {
	core       utils.QueryGenerator
	numMetrics int
}

// NewGroupBy produces a function that produces a new Groupby for the given parameters
func NewGroupBy(numMetrics int) utils.QueryFillerMaker {
	return func(core utils.QueryGenerator) utils.QueryFiller {
		return &Groupby{
			core:       core,
			numMetrics: numMetrics,
		}
	}
}

// Fill fills in the query.Query with query details
func (d *Groupby) Fill(q query.Query) query.Query {
	fc, ok := d.core.(DoubleGroupbyFiller)
	if !ok {
		common.PanicUnimplementedQuery(d.core)
	}
	fc.GroupByTimeAndPrimaryTag(q, d.numMetrics)
	return q
}
