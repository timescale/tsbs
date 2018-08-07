package devops

import (
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_data/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_data/serialize"
)

type subsystemMeasurement struct {
	timestamp     time.Time
	distributions []common.Distribution
}

func newSubsystemMeasurement(start time.Time, numDistributions int) *subsystemMeasurement {
	return &subsystemMeasurement{
		timestamp:     start,
		distributions: make([]common.Distribution, numDistributions),
	}
}

func newSubsystemMeasurementWithDistributionMakers(start time.Time, makers []labeledDistributionMaker) *subsystemMeasurement {
	m := newSubsystemMeasurement(start, len(makers))
	for i := 0; i < len(makers); i++ {
		m.distributions[i] = makers[i].distributionMaker()
	}
	return m
}

func (m *subsystemMeasurement) Tick(d time.Duration) {
	m.timestamp = m.timestamp.Add(d)
	for i := range m.distributions {
		m.distributions[i].Advance()
	}
}

func (m *subsystemMeasurement) toPoint(p *serialize.Point, measurementName []byte, labels []labeledDistributionMaker) {
	p.SetMeasurementName(measurementName)
	p.SetTimestamp(&m.timestamp)

	for i, d := range m.distributions {
		p.AppendField(labels[i].label, d.Get())
	}
}

// toPointAllInt64 fills in a serialize.Point with a given measurementName and
// all vales from the distributions stored as int64. The labels for each field
// are given by the supplied []labeledDistributionMaker, assuming that the distributions
// are in the same order.
func (m *subsystemMeasurement) toPointAllInt64(p *serialize.Point, measurementName []byte, labels []labeledDistributionMaker) {
	p.SetMeasurementName(measurementName)
	p.SetTimestamp(&m.timestamp)

	for i, d := range m.distributions {
		p.AppendField(labels[i].label, int64(d.Get()))
	}
}

type labeledDistributionMaker struct {
	label             []byte
	distributionMaker func() common.Distribution
}
