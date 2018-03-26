package main

import (
	"math"
	"math/rand"
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/cmd/tsbs_generate_data/serialize"
)

var (
	MemoryByteString = []byte("mem") // heap optimization

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
	// this doesn't change:
	bytesTotal int64

	// these change:
	timestamp                                         time.Time
	bytesUsedDist, bytesCachedDist, bytesBufferedDist Distribution
}

func NewMemMeasurement(start time.Time) *MemMeasurement {
	bytesTotal := MemoryMaxBytesChoices[rand.Intn(len(MemoryMaxBytesChoices))]
	bytesUsedDist := &ClampedRandomWalkDistribution{
		State: rand.Float64() * float64(bytesTotal),
		Min:   0.0,
		Max:   float64(bytesTotal),
		Step: &NormalDistribution{
			Mean:   0.0,
			StdDev: float64(bytesTotal) / 64,
		},
	}
	bytesCachedDist := &ClampedRandomWalkDistribution{
		State: rand.Float64() * float64(bytesTotal),
		Min:   0.0,
		Max:   float64(bytesTotal),
		Step: &NormalDistribution{
			Mean:   0.0,
			StdDev: float64(bytesTotal) / 64,
		},
	}
	bytesBufferedDist := &ClampedRandomWalkDistribution{
		State: rand.Float64() * float64(bytesTotal),
		Min:   0.0,
		Max:   float64(bytesTotal),
		Step: &NormalDistribution{
			Mean:   0.0,
			StdDev: float64(bytesTotal) / 64,
		},
	}
	return &MemMeasurement{
		timestamp: start,

		bytesTotal:        bytesTotal,
		bytesUsedDist:     bytesUsedDist,
		bytesCachedDist:   bytesCachedDist,
		bytesBufferedDist: bytesBufferedDist,
	}
}

func (m *MemMeasurement) Tick(d time.Duration) {
	m.timestamp = m.timestamp.Add(d)

	m.bytesUsedDist.Advance()
	m.bytesCachedDist.Advance()
	m.bytesBufferedDist.Advance()
}

func (m *MemMeasurement) ToPoint(p *serialize.Point) {
	p.SetMeasurementName(MemoryByteString)
	p.SetTimestamp(&m.timestamp)

	total := m.bytesTotal
	used := m.bytesUsedDist.Get()
	cached := m.bytesCachedDist.Get()
	buffered := m.bytesBufferedDist.Get()

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
