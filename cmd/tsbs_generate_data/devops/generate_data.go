package devops

import (
	"time"

	"bitbucket.org/440-labs/tsbs/cmd/tsbs_generate_data/common"
	"bitbucket.org/440-labs/tsbs/cmd/tsbs_generate_data/serialize"
)

// DevopsSimulator generates data similar to telemetry, with metrics from a variety of device systems.
// It fulfills the Simulator interface.
type DevopsSimulator struct {
	madePoints uint64
	maxPoints  uint64

	simulatedMeasurementIndex int

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
func (d *DevopsSimulator) Finished() bool {
	return d.madePoints >= d.maxPoints
}

// DevopsSimulatorConfig is used to create a DevopsSimulator.
type DevopsSimulatorConfig struct {
	Start time.Time
	End   time.Time

	InitHostCount   uint64
	HostCount       uint64
	HostConstructor func(i int, start time.Time) Host
}

func (d *DevopsSimulatorConfig) ToSimulator(interval time.Duration) common.Simulator {
	hostInfos := make([]Host, d.HostCount)
	for i := 0; i < len(hostInfos); i++ {
		hostInfos[i] = d.HostConstructor(i, d.Start)
	}

	epochs := uint64(d.End.Sub(d.Start).Nanoseconds() / interval.Nanoseconds())
	maxPoints := epochs * d.HostCount * uint64(len(hostInfos[0].SimulatedMeasurements))
	dg := &DevopsSimulator{
		madePoints: 0,
		maxPoints:  maxPoints,

		simulatedMeasurementIndex: 0,

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

func (d *DevopsSimulator) Fields() map[string][][]byte {
	data := make(map[string][][]byte)
	for _, sm := range d.hosts[0].SimulatedMeasurements {
		point := common.MakeUsablePoint()
		sm.ToPoint(point)
		data[string(point.MeasurementName())] = point.FieldKeys()
	}

	return data
}

// Next advances a Point to the next state in the generator.
func (d *DevopsSimulator) Next(p *serialize.Point) bool {
	// switch to the next metric if needed
	if d.hostIndex == uint64(len(d.hosts)) {
		d.hostIndex = 0
		d.simulatedMeasurementIndex++
	}

	if d.simulatedMeasurementIndex == len(d.hosts[0].SimulatedMeasurements) {
		d.simulatedMeasurementIndex = 0

		for i := 0; i < len(d.hosts); i++ {
			d.hosts[i].TickAll(d.interval)
		}
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
	host.SimulatedMeasurements[d.simulatedMeasurementIndex].ToPoint(p)

	ret := d.hostIndex < d.epochHosts
	d.madePoints++
	d.hostIndex++

	return ret
}
