package devops

import (
	"math"
	"math/rand"
	"time"

	"bitbucket.org/440-labs/tsbs/cmd/tsbs_generate_data/common"
	"bitbucket.org/440-labs/tsbs/cmd/tsbs_generate_data/serialize"
)

var (
	CPUByteString = []byte("cpu") // heap optimization
	CPUFieldKeys  = [][]byte{
		[]byte("usage_user"),
		[]byte("usage_system"),
		[]byte("usage_idle"),
		[]byte("usage_nice"),
		[]byte("usage_iowait"),
		[]byte("usage_irq"),
		[]byte("usage_softirq"),
		[]byte("usage_steal"),
		[]byte("usage_guest"),
		[]byte("usage_guest_nice"),
	}
)

var cpuND = &common.NormalDistribution{Mean: 0.0, StdDev: 1.0}

type CPUMeasurement struct {
	timestamp     time.Time
	distributions []common.Distribution
}

func NewCPUMeasurement(start time.Time) *CPUMeasurement {
	return newCPUMeasurementNumDistributions(start, len(CPUFieldKeys))
}

func newSingleCPUMeasurement(start time.Time) *CPUMeasurement {
	return newCPUMeasurementNumDistributions(start, 1)
}

func newCPUMeasurementNumDistributions(start time.Time, numDistributions int) *CPUMeasurement {
	distributions := make([]common.Distribution, numDistributions)
	for i := range distributions {
		distributions[i] = &common.ClampedRandomWalkDistribution{
			State: rand.Float64() * 100.0,
			Min:   0.0,
			Max:   100.0,
			Step:  cpuND,
		}
	}
	return &CPUMeasurement{
		timestamp:     start,
		distributions: distributions,
	}
}

func (m *CPUMeasurement) Tick(d time.Duration) {
	m.timestamp = m.timestamp.Add(d)
	for i := range m.distributions {
		m.distributions[i].Advance()
	}
}

func (m *CPUMeasurement) ToPoint(p *serialize.Point) {
	p.SetMeasurementName(CPUByteString)
	p.SetTimestamp(&m.timestamp)

	for i := range m.distributions {
		// Use ints for CPU metrics.
		// The full float64 precision in the distributions list is bad for compression on some systems (e.g., ZFS).
		// Anything above int precision is also not that common or useful for a devops CPU monitoring use case.
		p.AppendField(CPUFieldKeys[i], math.Round(m.distributions[i].Get()))
	}
}
