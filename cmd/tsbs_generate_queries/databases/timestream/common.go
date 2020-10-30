package timestream

import (
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
)

const goTimeFmt = "2006-01-02 15:04:05.999999 -0700"

// BaseGenerator contains settings specific for Timestream
type BaseGenerator struct {
	DBName string
}

// GenerateEmptyQuery returns an empty query.TimescaleDB.
func (g *BaseGenerator) GenerateEmptyQuery() query.Query {
	return query.NewTimestream()
}

// fillInQuery fills the query struct with data.
func (g *BaseGenerator) fillInQuery(qi query.Query, humanLabel, humanDesc, table, sql string) {
	q := qi.(*query.Timestream)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(humanDesc)
	q.SqlQuery = []byte(sql)
	q.Table = []byte(table)
}

// NewDevops creates a new devops use case query generator.
func (g *BaseGenerator) NewDevops(start, end time.Time, scale int) (utils.QueryGenerator, error) {
	core, err := devops.NewCore(start, end, scale)

	if err != nil {
		return nil, err
	}

	dOps := &Devops{
		BaseGenerator: g,
		Core:          core,
	}

	return dOps, nil
}
