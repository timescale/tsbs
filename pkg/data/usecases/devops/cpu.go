package devops

import (
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"math/rand"
	"time"
)

var (
	labelCPU  = []byte("cpu") // heap optimization
	cpuFields = []common.LabeledDistributionMaker{
		{Label: []byte("usage_user"), DistributionMaker: func() common.Distribution { return common.CWD(cpuND, 0.0, 100.0, rand.Float64()*100.0) }},
		{Label: []byte("usage_system"), DistributionMaker: func() common.Distribution { return common.CWD(cpuND, 0.0, 100.0, rand.Float64()*100.0) }},
		{Label: []byte("usage_idle"), DistributionMaker: func() common.Distribution { return common.CWD(cpuND, 0.0, 100.0, rand.Float64()*100.0) }},
		{Label: []byte("usage_nice"), DistributionMaker: func() common.Distribution { return common.CWD(cpuND, 0.0, 100.0, rand.Float64()*100.0) }},
		{Label: []byte("usage_iowait"), DistributionMaker: func() common.Distribution { return common.CWD(cpuND, 0.0, 100.0, rand.Float64()*100.0) }},
		{Label: []byte("usage_irq"), DistributionMaker: func() common.Distribution { return common.CWD(cpuND, 0.0, 100.0, rand.Float64()*100.0) }},
		{Label: []byte("usage_softirq"), DistributionMaker: func() common.Distribution { return common.CWD(cpuND, 0.0, 100.0, rand.Float64()*100.0) }},
		{Label: []byte("usage_steal"), DistributionMaker: func() common.Distribution { return common.CWD(cpuND, 0.0, 100.0, rand.Float64()*100.0) }},
		{Label: []byte("usage_guest"), DistributionMaker: func() common.Distribution { return common.CWD(cpuND, 0.0, 100.0, rand.Float64()*100.0) }},
		{Label: []byte("usage_guest_nice"), DistributionMaker: func() common.Distribution { return common.CWD(cpuND, 0.0, 100.0, rand.Float64()*100.0) }},
	}
)

// Reuse NormalDistributions as arguments to other distributions. This is
// safe to do because the higher-level distribution advances the ND and
// immediately uses its value and saves the state
var cpuND = common.ND(0.0, 1.0)

type CPUMeasurement struct {
	*common.SubsystemMeasurement
}

func NewCPUMeasurement(start time.Time) *CPUMeasurement {
	return newCPUMeasurementNumDistributions(start, len(cpuFields))
}

func newSingleCPUMeasurement(start time.Time) *CPUMeasurement {
	return newCPUMeasurementNumDistributions(start, 1)
}

func newCPUMeasurementNumDistributions(start time.Time, numDistributions int) *CPUMeasurement {
	sub := common.NewSubsystemMeasurementWithDistributionMakers(start, cpuFields[:numDistributions])
	return &CPUMeasurement{sub}
}

func (m *CPUMeasurement) ToPoint(p *data.Point) {
	m.ToPointAllInt64(p, labelCPU, cpuFields)
}
