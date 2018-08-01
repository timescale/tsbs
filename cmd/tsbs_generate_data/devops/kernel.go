package devops

import (
	"math/rand"
	"time"

	"bitbucket.org/440-labs/tsbs/cmd/tsbs_generate_data/common"
	"bitbucket.org/440-labs/tsbs/cmd/tsbs_generate_data/serialize"
)

var (
	KernelByteString   = []byte("kernel") // heap optimization
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
	sub := newSubsystemMeasurement(start, len(KernelFields))
	for i := range KernelFields {
		sub.distributions[i] = KernelFields[i].distributionMaker()
	}

	bootTime := rand.Int63n(240)
	return &KernelMeasurement{
		subsystemMeasurement: sub,
		bootTime:             bootTime,
	}
}

func (m *KernelMeasurement) ToPoint(p *serialize.Point) {
	p.SetMeasurementName(KernelByteString)
	p.SetTimestamp(&m.timestamp)

	p.AppendField(BootTimeByteString, m.bootTime)
	for i := range m.distributions {
		p.AppendField(KernelFields[i].label, int64(m.distributions[i].Get()))
	}
}
