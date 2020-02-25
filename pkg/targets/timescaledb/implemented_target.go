package timescaledb

import (
	"github.com/timescale/tsbs/load"
	"github.com/timescale/tsbs/pkg/targets"
)

func NewTarget() targets.ImplementedTarget {
	return &timescaleTarget{}
}

type timescaleTarget struct {
}

func (t *timescaleTarget) Benchmark() load.Benchmark {
	return nil
}
