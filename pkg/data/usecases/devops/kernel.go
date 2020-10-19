package devops

import (
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"math/rand"
	"time"
)

var (
	labelKernel         = []byte("kernel") // heap optimization
	labelKernelBootTime = []byte("boot_time")

	// Reuse NormalDistributions as arguments to other distributions. This is
	// safe to do because the higher-level distribution advances the ND and
	// immediately uses its value and saves the state
	kernelND = common.ND(5, 1)

	kernelFields = []common.LabeledDistributionMaker{
		{Label: []byte("interrupts"), DistributionMaker: func() common.Distribution { return common.MWD(kernelND, 0) }},
		{Label: []byte("context_switches"), DistributionMaker: func() common.Distribution { return common.MWD(kernelND, 0) }},
		{Label: []byte("processes_forked"), DistributionMaker: func() common.Distribution { return common.MWD(kernelND, 0) }},
		{Label: []byte("disk_pages_in"), DistributionMaker: func() common.Distribution { return common.MWD(kernelND, 0) }},
		{Label: []byte("disk_pages_out"), DistributionMaker: func() common.Distribution { return common.MWD(kernelND, 0) }},
	}
)

type KernelMeasurement struct {
	*common.SubsystemMeasurement
	bootTime int64
}

func NewKernelMeasurement(start time.Time) *KernelMeasurement {
	sub := common.NewSubsystemMeasurementWithDistributionMakers(start, kernelFields)
	bootTime := rand.Int63n(240)
	return &KernelMeasurement{
		SubsystemMeasurement: sub,
		bootTime:             bootTime,
	}
}

func (m *KernelMeasurement) ToPoint(p *data.Point) {
	p.AppendField(labelKernelBootTime, m.bootTime)
	m.ToPointAllInt64(p, labelKernel, kernelFields)
}
