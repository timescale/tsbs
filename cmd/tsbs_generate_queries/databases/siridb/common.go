package siridb

import (
	"sync"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/query"
)

// BaseGenerator contains settings specific for SiriDB
type BaseGenerator struct {
	QueryPool sync.Pool
}

// GenerateEmptyQuery returns an empty query.SiriDB.
func (g *BaseGenerator) GenerateEmptyQuery() query.Query {
	return g.QueryPool.Get().(*query.SiriDB)
}

// Releases the query to the generator's query pool
func (g *BaseGenerator) ReleaseQuery(q query.Query) {
	q.Release()
	g.QueryPool.Put(q)
}

// fillInQuery fills the query struct with data.
func (g *BaseGenerator) fillInQuery(qi query.Query, humanLabel, humanDesc, sql string) {
	q := qi.(*query.SiriDB)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(humanDesc)
	q.SqlQuery = []byte(sql)
}

// NewDevops creates a new devops use case query generator.
func (g *BaseGenerator) NewDevops(start, end time.Time, scale int) (utils.QueryGenerator, error) {
	core, err := devops.NewCore(start, end, scale)

	if err != nil {
		return nil, err
	}

	devops := &Devops{
		BaseGenerator: g,
		Core:          core,
	}

	return devops, nil
}
