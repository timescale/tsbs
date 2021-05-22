package victoriametrics

import (
	"fmt"
	"net/url"
	"strconv"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	iutils "github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/pkg/query"
)

type BaseGenerator struct{}

// GenerateEmptyQuery returns an empty query.HTTP.
func (g *BaseGenerator) GenerateEmptyQuery() query.Query {
	return query.NewHTTP()
}

// NewDevops creates a new devops use case query generator.
func (g *BaseGenerator) NewDevops(start, end time.Time, scale int) (utils.QueryGenerator, error) {
	core, err := devops.NewCore(start, end, scale)
	if err != nil {
		return nil, err
	}
	return &Devops{
		BaseGenerator: g,
		Core:          core,
	}, nil
}

type queryInfo struct {
	// prometheus query
	query string
	// label to describe type of query
	label string
	// desc to describe type of query
	desc string
	// time range for query executing
	interval *iutils.TimeInterval
	// time period to group by in seconds
	step string
}

// fill Query fills the query struct with data
func (g *BaseGenerator) fillInQuery(qq query.Query, qi *queryInfo) {
	q := qq.(*query.HTTP)
	q.HumanLabel = []byte(qi.label)
	if qi.interval != nil {
		q.HumanDescription = []byte(fmt.Sprintf("%s: %s", qi.label, qi.interval.StartString()))
	}
	q.Method = []byte("GET")

	v := url.Values{}
	v.Set("query", qi.query)
	v.Set("start", strconv.FormatInt(qi.interval.StartUnixNano()/1e9, 10))
	v.Set("end", strconv.FormatInt(qi.interval.EndUnixNano()/1e9, 10))
	v.Set("step", qi.step)
	q.Path = []byte(fmt.Sprintf("/api/v1/query_range?%s", v.Encode()))
	q.Body = nil
}
