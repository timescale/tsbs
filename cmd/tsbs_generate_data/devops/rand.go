package devops

import (
	"time"

	"bitbucket.org/440-labs/tsbs/cmd/tsbs_generate_data/common"
	"bitbucket.org/440-labs/tsbs/cmd/tsbs_generate_data/serialize"
)

var (
	RandByteString = []byte("rand") // heap optimization

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
	sub := newSubsystemMeasurement(start, len(RandFields))
	for i := range RandFields {
		sub.distributions[i] = common.UD(0.0, 100.0)
	}
	return &RandMeasurement{sub}
}

func (m *RandMeasurement) ToPoint(p *serialize.Point) {
	p.SetMeasurementName(RandByteString)
	p.SetTimestamp(&m.timestamp)

	for i := range m.distributions {
		p.AppendField(RandFields[i].label, m.distributions[i].Get())
	}
}
