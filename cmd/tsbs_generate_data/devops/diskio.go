package devops

import (
	"fmt"
	"math/rand"
	"time"

	"bitbucket.org/440-labs/tsbs/cmd/tsbs_generate_data/common"
	"bitbucket.org/440-labs/tsbs/cmd/tsbs_generate_data/serialize"
)

var (
	DiskIOByteString = []byte("diskio") // heap optimization
	SerialByteString = []byte("serial")

	DiskIOFields = []LabeledDistributionMaker{
		{[]byte("reads"), func() common.Distribution { return common.MWD(common.ND(50, 1), 0) }},
		{[]byte("writes"), func() common.Distribution { return common.MWD(common.ND(50, 1), 0) }},
		{[]byte("read_bytes"), func() common.Distribution { return common.MWD(common.ND(100, 1), 0) }},
		{[]byte("write_bytes"), func() common.Distribution { return common.MWD(common.ND(100, 1), 0) }},
		{[]byte("read_time"), func() common.Distribution { return common.MWD(common.ND(5, 1), 0) }},
		{[]byte("write_time"), func() common.Distribution { return common.MWD(common.ND(5, 1), 0) }},
		{[]byte("io_time"), func() common.Distribution { return common.MWD(common.ND(5, 1), 0) }},
	}
)

type DiskIOMeasurement struct {
	timestamp time.Time

	serial        []byte
	distributions []common.Distribution
}

func NewDiskIOMeasurement(start time.Time) *DiskIOMeasurement {
	distributions := make([]common.Distribution, len(DiskIOFields))
	for i := range DiskIOFields {
		distributions[i] = DiskIOFields[i].DistributionMaker()
	}

	serial := []byte(fmt.Sprintf("%03d-%03d-%03d", rand.Intn(1000), rand.Intn(1000), rand.Intn(1000)))
	return &DiskIOMeasurement{
		serial: serial,

		timestamp:     start,
		distributions: distributions,
	}
}

func (m *DiskIOMeasurement) Tick(d time.Duration) {
	m.timestamp = m.timestamp.Add(d)

	for i := range m.distributions {
		m.distributions[i].Advance()
	}
}

func (m *DiskIOMeasurement) ToPoint(p *serialize.Point) {
	p.SetMeasurementName(DiskIOByteString)
	p.SetTimestamp(&m.timestamp)

	p.AppendTag(SerialByteString, m.serial)

	for i := range m.distributions {
		p.AppendField(DiskIOFields[i].Label, int64(m.distributions[i].Get()))
	}
}
