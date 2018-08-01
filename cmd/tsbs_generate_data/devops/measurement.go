package devops

import (
	"time"

	"bitbucket.org/440-labs/tsbs/cmd/tsbs_generate_data/common"
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

func (m *subsystemMeasurement) Tick(d time.Duration) {
	m.timestamp = m.timestamp.Add(d)
	for i := range m.distributions {
		m.distributions[i].Advance()
	}
}

type labeledDistributionMaker struct {
	label             []byte
	distributionMaker func() common.Distribution
}
