package iotdb

import (
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
)

const iotdbTimeFmt = "2006-01-02 15:04:05"

// BaseGenerator contains settings specific for IoTDB
type BaseGenerator struct {
	BasicPath      string // e.g. "root.sg" is basic path of "root.sg.device"
	BasicPathLevel int32  // e.g. 0 for "root", 1 for "root.device"
}

// GenerateEmptyQuery returns an empty query.Mongo.
func (g *BaseGenerator) GenerateEmptyQuery() query.Query {
	return query.NewIoTDB()
}

// fillInQuery fills the query struct with data.
func (g *BaseGenerator) fillInQuery(qi query.Query, humanLabel, humanDesc, sql string) {
	q := qi.(*query.IoTDB)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(humanDesc)
	q.SqlQuery = []byte(sql)
	// CRTODO: 在修改了结构之后，这里是否还需要更多的东西？
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
