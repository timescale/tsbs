package hyprcubd

import (
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/query"
)

// BaseGenerator contains settings specific for Hyprcubd
type BaseGenerator struct{}

// GenerateEmptyQuery returns an empty query.HTTP
func (g *BaseGenerator) GenerateEmptyQuery() query.Query {
	return query.NewHTTP()
}

func (g *BaseGenerator) fillInQuery(qq query.Query, label, description, rawQuery string) {
	q := qq.(*query.HTTP)
	q.HumanLabel = []byte(label)
	q.RawQuery = []byte(rawQuery)
	q.HumanDescription = []byte(description)
	q.Method = []byte("POST")
	q.Path = nil
	q.Body = nil
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
