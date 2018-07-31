package devops

import (
	"time"

	"bitbucket.org/440-labs/tsbs/cmd/tsbs_generate_data/common"
	"bitbucket.org/440-labs/tsbs/cmd/tsbs_generate_data/serialize"
)

// A CPUOnlySimulator generates data similar to telemetry from Telegraf for only CPU metrics.
// It fulfills the Simulator interface.
type CPUOnlySimulator struct {
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
func (d *CPUOnlySimulator) Finished() bool {
	return d.madePoints >= d.maxPoints
}

// CPUOnlySimulatorConfig is used to create a CPUOnlySimulator.
type CPUOnlySimulatorConfig struct {
	Start time.Time
	End   time.Time

	// InitHostCount is the number of hosts to start with in the first reporting period
	InitHostCount uint64
	// HostCount is the total number of hosts to have in the last reporting period
	HostCount       uint64
	HostConstructor func(i int, start time.Time) Host
}

func (d *CPUOnlySimulatorConfig) ToSimulator(interval time.Duration) common.Simulator {
	hostInfos := make([]Host, d.HostCount)
	for i := 0; i < len(hostInfos); i++ {
		hostInfos[i] = d.HostConstructor(i, d.Start)
	}

	epochs := uint64(d.End.Sub(d.Start).Nanoseconds() / interval.Nanoseconds())
	maxPoints := epochs * d.HostCount
	dg := &CPUOnlySimulator{
		madePoints: 0,
		maxPoints:  maxPoints,

		hostIndex: 0,
		hosts:     hostInfos,

		epoch:          0,
		epochs:         epochs,
		epochHosts:     d.InitHostCount,
		initHosts:      d.InitHostCount,
		timestampStart: d.Start,
		timestampEnd:   d.End,
		interval:       interval,
	}

	return dg
}

func (d *CPUOnlySimulator) Fields() map[string][][]byte {
	data := make(map[string][][]byte)
	point := serialize.NewPoint()
	d.hosts[0].SimulatedMeasurements[0].ToPoint(point)
	data[string(point.MeasurementName())] = point.FieldKeys()

	return data
}

// Next advances a Point to the next state in the generator.
func (d *CPUOnlySimulator) Next(p *serialize.Point) bool {
	// switch to the next metric if needed
	if d.hostIndex == uint64(len(d.hosts)) {
		d.hostIndex = 0

		for i := 0; i < len(d.hosts); i++ {
			d.hosts[i].TickAll(d.interval)
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
		d.epoch++
		missingScale := float64(uint64(len(d.hosts)) - d.initHosts)
		d.epochHosts = d.initHosts + uint64(missingScale*float64(d.epoch)/float64(d.epochs-1))

	}

	host := &d.hosts[d.hostIndex]

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
	host.SimulatedMeasurements[0].ToPoint(p)

	ret := d.hostIndex < d.epochHosts
	d.madePoints++
	d.hostIndex++

	return ret
}
