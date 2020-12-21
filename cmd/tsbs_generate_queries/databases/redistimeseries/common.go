package redistimeseries

import (
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/query"
	"time"
)

// BaseGenerator contains settings specific for RedisTimeSeries database.
type BaseGenerator struct {
}

// GenerateEmptyQuery returns an empty query.Cassandra.
func (g *BaseGenerator) GenerateEmptyQuery() query.Query {
	return query.NewRedisTimeSeries()
}

// fill Query fills the query struct with data
func (d *BaseGenerator) fillInQueryStrings(qi query.Query, humanLabel, humanDesc string) {
	q := qi.(*query.RedisTimeSeries)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(humanDesc)
}

// AddQuery adds a command to be executed in the full flow of this Query
func (d *BaseGenerator) AddQuery(qi query.Query, tq [][]byte, commandname []byte) {
	q := qi.(*query.RedisTimeSeries)
	q.AddQuery(tq, commandname)
}

// SetApplyFunctor sets SetApplyFunctor used for this Query
func (d *BaseGenerator) SetApplyFunctor(qi query.Query, value bool, functor string) {
	q := qi.(*query.RedisTimeSeries)
	q.SetApplyFunctor(value)
	q.SetFunctor(functor)
}

// NewDevops creates a new devops use case query generator.
func (g *BaseGenerator) NewDevops(start, end time.Time, scale int) (utils.QueryGenerator, error) {
	core, err := devops.NewCore(start, end, scale)

	if err != nil {
		return nil, err
	}

	var devops utils.QueryGenerator = &Devops{
		BaseGenerator: g,
		Core:          core,
	}

	return devops, nil
}
