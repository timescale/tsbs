package common

import (
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_data/serialize"
)

// SubsystemMeasurement represents a collection of measurement distributions and a start time.
type SubsystemMeasurement struct {
	Timestamp     time.Time
	Distributions []Distribution
}

// NewSubsystemMeasurement creates a new SubsystemMeasurement with provided start time and number of distributions.
func NewSubsystemMeasurement(start time.Time, numDistributions int) *SubsystemMeasurement {
	return &SubsystemMeasurement{
		Timestamp:     start,
		Distributions: make([]Distribution, numDistributions),
	}
}

// NewSubsystemMeasurementWithDistributionMakers creates a new SubsystemMeasurement with start time and distribution makers
// which are used to create the necessary distributions.
func NewSubsystemMeasurementWithDistributionMakers(start time.Time, makers []LabeledDistributionMaker) *SubsystemMeasurement {
	m := NewSubsystemMeasurement(start, len(makers))
	for i := 0; i < len(makers); i++ {
		m.Distributions[i] = makers[i].DistributionMaker()
	}
	return m
}

// Tick advances all the distributions for the SubsystemMeasurement.
func (m *SubsystemMeasurement) Tick(d time.Duration) {
	m.Timestamp = m.Timestamp.Add(d)
	for i := range m.Distributions {
		m.Distributions[i].Advance()
	}
}

// ToPoint fills the provided serialize.Point with measurements from the SubsystemMeasurement.
func (m *SubsystemMeasurement) ToPoint(p *serialize.Point, measurementName []byte, labels []LabeledDistributionMaker) {
	p.SetMeasurementName(measurementName)
	p.SetTimestamp(&m.Timestamp)

	for i, d := range m.Distributions {
		p.AppendField(labels[i].Label, d.Get())
	}
}

// ToPointAllInt64 fills in a serialize.Point with a given measurementName and
// all vales from the distributions stored as int64. The labels for each field
// are given by the supplied []LabeledDistributionMaker, assuming that the distributions
// are in the same order.
func (m *SubsystemMeasurement) ToPointAllInt64(p *serialize.Point, measurementName []byte, labels []LabeledDistributionMaker) {
	p.SetMeasurementName(measurementName)
	p.SetTimestamp(&m.Timestamp)

	for i, d := range m.Distributions {
		p.AppendField(labels[i].Label, int64(d.Get()))
	}
}

// LabeledDistributionMaker combines a distribution maker with a label.
type LabeledDistributionMaker struct {
	Label             []byte
	DistributionMaker func() Distribution
}
