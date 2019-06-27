package cassandra

import (
	"strings"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	internalutils "github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/query"
)

// BaseGenerator contains settings specific for Cassandra database.
type BaseGenerator struct {
}

// GenerateEmptyQuery returns an empty query.Cassandra.
func (g *BaseGenerator) GenerateEmptyQuery() query.Query {
	return query.NewCassandra()
}

// fillInQuery fills the query struct with data.
func (g *BaseGenerator) fillInQuery(qi query.Query, humanLabel, humanDesc, aggType string, fields []string, interval *internalutils.TimeInterval, tagSets [][]string) {
	q := qi.(*query.Cassandra)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(humanDesc)

	q.AggregationType = []byte(aggType)
	q.MeasurementName = []byte("cpu")
	q.FieldName = []byte(strings.Join(fields, ","))

	q.TimeStart = interval.Start()
	q.TimeEnd = interval.End()

	q.TagSets = tagSets
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
