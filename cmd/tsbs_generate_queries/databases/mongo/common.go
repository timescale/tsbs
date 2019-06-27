package mongo

import (
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/query"
)

// BaseGenerator contains settings specific for Mongo database.
type BaseGenerator struct {
	UseNaive bool
}

// GenerateEmptyQuery returns an empty query.Mongo.
func (g *BaseGenerator) GenerateEmptyQuery() query.Query {
	return query.NewMongo()
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

	if g.UseNaive {
		devops = &NaiveDevops{
			BaseGenerator: g,
			Core:          core,
		}

	}

	return devops, nil
}
