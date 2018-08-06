package devops

import (
	"math"
	"math/rand"
	"time"

	"bitbucket.org/440-labs/tsbs/cmd/tsbs_generate_data/common"
	"bitbucket.org/440-labs/tsbs/cmd/tsbs_generate_data/serialize"
)

var (
	labelMem = []byte("mem") // heap optimization

	// Choices for modeling a host's memory capacity.
	MemoryMaxBytesChoices = []int64{8 << 30, 12 << 30, 16 << 30}

	// Field keys for 'mem' points.
	MemoryFieldKeys = [][]byte{
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
	bytesTotal := MemoryMaxBytesChoices[rand.Intn(len(MemoryMaxBytesChoices))]

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
	used := m.distributions[0].Get()
	cached := m.distributions[1].Get()
	buffered := m.distributions[2].Get()

	p.AppendField(MemoryFieldKeys[0], total)
	p.AppendField(MemoryFieldKeys[1], int(math.Floor(float64(total)-used)))
	p.AppendField(MemoryFieldKeys[2], int(math.Floor(used)))
	p.AppendField(MemoryFieldKeys[3], int(math.Floor(cached)))
	p.AppendField(MemoryFieldKeys[4], int(math.Floor(buffered)))
	p.AppendField(MemoryFieldKeys[5], int(math.Floor(used)))
	p.AppendField(MemoryFieldKeys[6], 100.0*(used/float64(total)))
	p.AppendField(MemoryFieldKeys[7], 100.0*(float64(total)-used)/float64(total))
	p.AppendField(MemoryFieldKeys[8], 100.0*(float64(total)-buffered)/float64(total))
}
