package devops

import (
	"reflect"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_data/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_data/serialize"
)

type commonDevopsSimulatorConfig struct {
	// Start is the beginning time for the Simulator
	Start time.Time
	// End is the ending time for the Simulator
	End time.Time
	// InitHostCount is the number of hosts to start with in the first reporting period
	InitHostCount uint64
	// HostCount is the total number of hosts to have in the last reporting period
	HostCount uint64
	// HostConstructor is the function used to create a new Host given an id number and start time
	HostConstructor func(i int, start time.Time) Host
}

func calculateEpochs(c commonDevopsSimulatorConfig, interval time.Duration) uint64 {
	return uint64(c.End.Sub(c.Start).Nanoseconds() / interval.Nanoseconds())
}

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
	if len(s.hosts) <= 0 {
		panic("cannot get fields because no hosts added")
	}
	return s.fields(s.hosts[0].SimulatedMeasurements)
}

func (s *commonDevopsSimulator) TagKeys() [][]byte {
	return MachineTagKeys
}

func (s *commonDevopsSimulator) TagTypes() []reflect.Type {
	types := make([]reflect.Type, len(MachineTagKeys))
	for i := 0; i < len(MachineTagKeys); i++ {
		types[i] = MachineTagType
	}
	return types
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
