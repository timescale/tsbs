package devops

import (
	"time"

	"bitbucket.org/440-labs/tsbs/cmd/tsbs_generate_data/common"
	"bitbucket.org/440-labs/tsbs/cmd/tsbs_generate_data/serialize"
)

type commonDevopsSimulator struct {
	madePoints uint64
	maxPoints  uint64

	hostIndex uint64
	hosts     []Host

	epoch      uint64
	epochs     uint64
	epochHosts uint64
	initHosts  uint64

	timestampStart time.Time
	timestampEnd   time.Time
	interval       time.Duration
}

// Finished tells whether we have simulated all the necessary points
func (s *commonDevopsSimulator) Finished() bool {
	return s.madePoints >= s.maxPoints
}

func (s *commonDevopsSimulator) Fields() map[string][][]byte {
	return s.fields(s.hosts[0].SimulatedMeasurements)
}

func (s *commonDevopsSimulator) fields(measurements []common.SimulatedMeasurement) map[string][][]byte {
	data := make(map[string][][]byte)
	for _, sm := range measurements {
		point := serialize.NewPoint()
		sm.ToPoint(point)
		data[string(point.MeasurementName())] = point.FieldKeys()
	}

	return data
}

func (s *commonDevopsSimulator) populatePoint(p *serialize.Point, measureIdx int) bool {
	host := &s.hosts[s.hostIndex]

	// Populate host-specific tags:
	p.AppendTag(MachineTagKeys[0], host.Name)
	p.AppendTag(MachineTagKeys[1], host.Region)
	p.AppendTag(MachineTagKeys[2], host.Datacenter)
	p.AppendTag(MachineTagKeys[3], host.Rack)
	p.AppendTag(MachineTagKeys[4], host.OS)
	p.AppendTag(MachineTagKeys[5], host.Arch)
	p.AppendTag(MachineTagKeys[6], host.Team)
	p.AppendTag(MachineTagKeys[7], host.Service)
	p.AppendTag(MachineTagKeys[8], host.ServiceVersion)
	p.AppendTag(MachineTagKeys[9], host.ServiceEnvironment)

	// Populate measurement-specific tags and fields:
	host.SimulatedMeasurements[measureIdx].ToPoint(p)

	ret := s.hostIndex < s.epochHosts
	s.madePoints++
	s.hostIndex++
	return ret
}

// TODO(rrk) - Can probably turn this logic into a separate interface and implement other
// types of scale up, e.g., exponential
//
// To "scale up" the number of reporting items, we need to know when
// which epoch we are currently in. Once we know that, we can take the "missing"
// amount of scale -- i.e., the max amount of scale less the initial amount
// -- and add it in proportion to the percentage of epochs that have passed. This
// way we simulate all items at each epoch, but at the end of the function
// we check whether the point should be recorded by the calling process.
func (s *commonDevopsSimulator) adjustNumHostsForEpoch() {
	s.epoch++
	missingScale := float64(uint64(len(s.hosts)) - s.initHosts)
	s.epochHosts = s.initHosts + uint64(missingScale*float64(s.epoch)/float64(s.epochs-1))
}
