package devops

import (
	"math/rand"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_data/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_data/serialize"
)

var (
	labelKernel        = []byte("kernel") // heap optimization
	BootTimeByteString = []byte("boot_time")

	// Reuse NormalDistributions as arguments to other distributions. This is
	// safe to do because the higher-level distribution advances the ND and
	// immediately uses its value and saves the state
	kernelND = common.ND(5, 1)

	KernelFields = []labeledDistributionMaker{
		{[]byte("interrupts"), func() common.Distribution { return common.MWD(kernelND, 0) }},
		{[]byte("context_switches"), func() common.Distribution { return common.MWD(kernelND, 0) }},
		{[]byte("processes_forked"), func() common.Distribution { return common.MWD(kernelND, 0) }},
		{[]byte("disk_pages_in"), func() common.Distribution { return common.MWD(kernelND, 0) }},
		{[]byte("disk_pages_out"), func() common.Distribution { return common.MWD(kernelND, 0) }},
	}
)

type KernelMeasurement struct {
	*subsystemMeasurement
	bootTime int64
}

func NewKernelMeasurement(start time.Time) *KernelMeasurement {
	sub := newSubsystemMeasurementWithDistributionMakers(start, KernelFields)
	bootTime := rand.Int63n(240)
	return &KernelMeasurement{
		subsystemMeasurement: sub,
		bootTime:             bootTime,
	}
}

func (m *KernelMeasurement) ToPoint(p *serialize.Point) {
	p.AppendField(BootTimeByteString, m.bootTime)
	m.toPointAllInt64(p, labelKernel, KernelFields)
}
