package devops

import (
	"bitbucket.org/440-labs/tsbs/cmd/tsbs_generate_queries/utils"
	"bitbucket.org/440-labs/tsbs/query"
)

// LastPointPerHost returns QueryFiller for the devops lastpoint case
type LastPointPerHost struct {
	core utils.DevopsGenerator
}

// NewLastPointPerHost returns a new LastPointPerHost for given paremeters
func NewLastPointPerHost(core utils.DevopsGenerator) utils.QueryFiller {
	return &LastPointPerHost{core}
}

// Fill fills in the query.Query with query details
func (d *LastPointPerHost) Fill(q query.Query) query.Query {
	fc, ok := d.core.(LastPointFiller)
	if !ok {
		panicUnimplementedQuery(d.core)
	}
	fc.LastPointPerHost(q)
	return q
}
