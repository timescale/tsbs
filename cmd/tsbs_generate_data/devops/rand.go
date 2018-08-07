package devops

import (
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_data/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_data/serialize"
)

var (
	labelRand = []byte("rand") // heap optimization

	RandFields = []labeledDistributionMaker{
		{[]byte("usage_user"), func() common.Distribution { return common.UD(0.0, 100.0) }},
		{[]byte("usage_system"), func() common.Distribution { return common.UD(0.0, 100.0) }},
		{[]byte("usage_idle"), func() common.Distribution { return common.UD(0.0, 100.0) }},
		{[]byte("usage_nice"), func() common.Distribution { return common.UD(0.0, 100.0) }},
		{[]byte("usage_iowait"), func() common.Distribution { return common.UD(0.0, 100.0) }},
		{[]byte("usage_irq"), func() common.Distribution { return common.UD(0.0, 100.0) }},
		{[]byte("usage_softirq"), func() common.Distribution { return common.UD(0.0, 100.0) }},
		{[]byte("usage_steal"), func() common.Distribution { return common.UD(0.0, 100.0) }},
		{[]byte("usage_guest"), func() common.Distribution { return common.UD(0.0, 100.0) }},
		{[]byte("usage_guest_nice"), func() common.Distribution { return common.UD(0.0, 100.0) }},
	}
)

type RandMeasurement struct {
	*subsystemMeasurement
}

func NewRandMeasurement(start time.Time) *RandMeasurement {
	sub := newSubsystemMeasurementWithDistributionMakers(start, RandFields)
	return &RandMeasurement{sub}
}

func (m *RandMeasurement) ToPoint(p *serialize.Point) {
	m.toPoint(p, labelRand, RandFields)
}
