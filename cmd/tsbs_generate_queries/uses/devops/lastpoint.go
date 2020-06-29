package devops

import (
	"github.com/iznauy/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/iznauy/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/iznauy/tsbs/query"
)

// LastPointPerHost returns QueryFiller for the devops lastpoint case
type LastPointPerHost struct {
	core utils.QueryGenerator
}

// NewLastPointPerHost returns a new LastPointPerHost for given paremeters
func NewLastPointPerHost(core utils.QueryGenerator) utils.QueryFiller {
	return &LastPointPerHost{core}
}

// Fill fills in the query.Query with query details
func (d *LastPointPerHost) Fill(q query.Query) query.Query {
	fc, ok := d.core.(LastPointFiller)
	if !ok {
		common.PanicUnimplementedQuery(d.core)
	}
	fc.LastPointPerHost(q)
	return q
}
