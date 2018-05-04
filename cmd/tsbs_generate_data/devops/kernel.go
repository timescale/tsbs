package devops

import (
	"math/rand"
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/cmd/tsbs_generate_data/common"
	"bitbucket.org/440-labs/influxdb-comparisons/cmd/tsbs_generate_data/serialize"
)

var (
	KernelByteString   = []byte("kernel") // heap optimization
	BootTimeByteString = []byte("boot_time")
	KernelFields       = []LabeledDistributionMaker{
		{[]byte("interrupts"), func() common.Distribution { return common.MWD(common.ND(5, 1), 0) }},
		{[]byte("context_switches"), func() common.Distribution { return common.MWD(common.ND(5, 1), 0) }},
		{[]byte("processes_forked"), func() common.Distribution { return common.MWD(common.ND(5, 1), 0) }},
		{[]byte("disk_pages_in"), func() common.Distribution { return common.MWD(common.ND(5, 1), 0) }},
		{[]byte("disk_pages_out"), func() common.Distribution { return common.MWD(common.ND(5, 1), 0) }},
	}
)

type KernelMeasurement struct {
	timestamp time.Time

	bootTime      int64
	uptime        time.Duration
	distributions []common.Distribution
}

func NewKernelMeasurement(start time.Time) *KernelMeasurement {
	distributions := make([]common.Distribution, len(KernelFields))
	for i := range KernelFields {
		distributions[i] = KernelFields[i].DistributionMaker()
	}

	bootTime := rand.Int63n(240)
	return &KernelMeasurement{
		bootTime: bootTime,

		timestamp:     start,
		distributions: distributions,
	}
}

func (m *KernelMeasurement) Tick(d time.Duration) {
	m.timestamp = m.timestamp.Add(d)

	for i := range m.distributions {
		m.distributions[i].Advance()
	}
}

func (m *KernelMeasurement) ToPoint(p *serialize.Point) {
	p.SetMeasurementName(KernelByteString)
	p.SetTimestamp(&m.timestamp)

	p.AppendField(BootTimeByteString, m.bootTime)
	for i := range m.distributions {
		p.AppendField(KernelFields[i].Label, int64(m.distributions[i].Get()))
	}
}
