package devops

import (
	"github.com/iznauy/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/iznauy/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/iznauy/tsbs/query"
)

// MaxAllCPU contains info for filling in a query.Query for "max all" queries
type MaxAllCPU struct {
	core  utils.QueryGenerator
	hosts int
}

// NewMaxAllCPU produces a new function that produces a new AllMaxCPU
func NewMaxAllCPU(hosts int) utils.QueryFillerMaker {
	return func(core utils.QueryGenerator) utils.QueryFiller {
		return &MaxAllCPU{
			core:  core,
			hosts: hosts,
		}
	}
}

// Fill fills in the query.Query with query details
func (d *MaxAllCPU) Fill(q query.Query) query.Query {
	fc, ok := d.core.(MaxAllFiller)
	if !ok {
		common.PanicUnimplementedQuery(d.core)
	}
	fc.MaxAllCPU(q, d.hosts)
	return q
}
