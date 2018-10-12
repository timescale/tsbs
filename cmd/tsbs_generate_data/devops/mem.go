package devops

import (
	"math/rand"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_data/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_data/serialize"
)

var (
	labelMem = []byte("mem") // heap optimization

	// memoryTotalChoices are the choices for modeling a host's total memory capacity.
	memoryTotalChoices = []int64{8 << 30, 12 << 30, 16 << 30}

	memoryFieldKeys = [][]byte{
		[]byte("total"),
		[]byte("available"),
		[]byte("used"),
		[]byte("free"),
		[]byte("cached"),
		[]byte("buffered"),
		[]byte("used_percent"),
		[]byte("available_percent"),
		[]byte("buffered_percent"),
	}
)

type MemMeasurement struct {
	*subsystemMeasurement
	bytesTotal int64 // this doesn't change
}

func NewMemMeasurement(start time.Time) *MemMeasurement {
	sub := newSubsystemMeasurement(start, 3)
	bytesTotal := randomInt64SliceChoice(memoryTotalChoices)

	// Reuse NormalDistributions as arguments to other distributions. This is
	// safe to do because the higher-level distribution advances the ND and
	// immediately uses its value and saves the state
	nd := common.ND(0.0, float64(bytesTotal)/64)

	// used bytes
	sub.distributions[0] = common.CWD(nd, 0.0, float64(bytesTotal), rand.Float64()*float64(bytesTotal))
	// cached bytes
	sub.distributions[1] = common.CWD(nd, 0.0, float64(bytesTotal), rand.Float64()*float64(bytesTotal))
	// buffered bytes
	sub.distributions[2] = common.CWD(nd, 0.0, float64(bytesTotal), rand.Float64()*float64(bytesTotal))
	return &MemMeasurement{
		subsystemMeasurement: sub,
		bytesTotal:           bytesTotal,
	}
}

func (m *MemMeasurement) ToPoint(p *serialize.Point) {
	p.SetMeasurementName(labelMem)
	p.SetTimestamp(&m.timestamp)

	total := m.bytesTotal
	used := int64(m.distributions[0].Get())
	cached := int64(m.distributions[1].Get())
	buffered := int64(m.distributions[2].Get())
	available := total - int64(used)

	p.AppendField(memoryFieldKeys[0], total)
	p.AppendField(memoryFieldKeys[1], available)
	p.AppendField(memoryFieldKeys[2], used)
	// TODO - This data model is broken since `free` is actually a different thing than available,
	// but since there is no other distribution currently suitable to represent `free` I made this
	// change from what it was before.
	p.AppendField(memoryFieldKeys[3], available)
	p.AppendField(memoryFieldKeys[4], cached)
	p.AppendField(memoryFieldKeys[5], buffered)
	p.AppendField(memoryFieldKeys[6], 100.0*(float64(used)/float64(total)))
	p.AppendField(memoryFieldKeys[7], 100.0*(float64(available)/float64(total)))
	p.AppendField(memoryFieldKeys[8], 100.0*(float64(buffered))/float64(total))
}
