package devops

import (
	"fmt"
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"math/rand"
	"time"
)

const (
	diskSerialFmt = "%03d-%03d-%03d"
)

var (
	labelDiskIO       = []byte("diskio") // heap optimization
	labelDiskIOSerial = []byte("serial")

	// Reuse NormalDistributions as arguments to other distributions. This is
	// safe to do because the higher-level distribution advances the ND and
	// immediately uses its value and saves the state
	opsND   = common.ND(50, 1)
	bytesND = common.ND(100, 1)
	timeND  = common.ND(5, 1)

	diskIOFields = []common.LabeledDistributionMaker{
		{[]byte("reads"), func() common.Distribution { return common.MWD(opsND, 0) }},
		{[]byte("writes"), func() common.Distribution { return common.MWD(opsND, 0) }},
		{[]byte("read_bytes"), func() common.Distribution { return common.MWD(bytesND, 0) }},
		{[]byte("write_bytes"), func() common.Distribution { return common.MWD(bytesND, 0) }},
		{[]byte("read_time"), func() common.Distribution { return common.MWD(timeND, 0) }},
		{[]byte("write_time"), func() common.Distribution { return common.MWD(timeND, 0) }},
		{[]byte("io_time"), func() common.Distribution { return common.MWD(timeND, 0) }},
	}
)

type DiskIOMeasurement struct {
	*common.SubsystemMeasurement
	serial string
}

func NewDiskIOMeasurement(start time.Time) *DiskIOMeasurement {
	sub := common.NewSubsystemMeasurementWithDistributionMakers(start, diskIOFields)
	serial := fmt.Sprintf(diskSerialFmt, rand.Intn(1000), rand.Intn(1000), rand.Intn(1000))
	return &DiskIOMeasurement{
		SubsystemMeasurement: sub,
		serial:               serial,
	}
}

func (m *DiskIOMeasurement) ToPoint(p *data.Point) {
	m.ToPointAllInt64(p, labelDiskIO, diskIOFields)
	p.AppendTag(labelDiskIOSerial, m.serial)
}
