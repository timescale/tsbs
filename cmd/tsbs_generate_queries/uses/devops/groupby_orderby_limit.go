package devops

import (
	"bitbucket.org/440-labs/tsbs/cmd/tsbs_generate_queries/utils"
	"bitbucket.org/440-labs/tsbs/query"
)

// GroupByOrderByLimit produces a filler for queries in the devops groupby-orderby-limit case.
type GroupByOrderByLimit struct {
	core utils.DevopsGenerator
}

// NewGroupByOrderByLimit returns a new GroupByOrderByLimit for given paremeters
func NewGroupByOrderByLimit(core utils.DevopsGenerator) utils.QueryFiller {
	return &GroupByOrderByLimit{core}
}

// Fill fills in the query.Query with query details
func (d *GroupByOrderByLimit) Fill(q query.Query) query.Query {
	fc, ok := d.core.(GroupbyOrderbyLimitFiller)
	if !ok {
		panicUnimplementedQuery(d.core)
	}
	fc.GroupByOrderByLimit(q)
	return q
}
