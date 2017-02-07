package main

import (
	"time"
)

// A DevopsSimulator generates data similar to telemetry from Telegraf.
// It fulfills the Simulator interface.
type DevopsSimulator struct {
	madePoints int64
	maxPoints  int64

	simulatedMeasurementIndex int

	hostIndex int
	hosts     []Host

	timestampNow   time.Time
	timestampStart time.Time
	timestampEnd   time.Time
}

func (g *DevopsSimulator) Seen() int64 {
	return g.madePoints
}

func (g *DevopsSimulator) Total() int64 {
	return g.maxPoints
}

func (g *DevopsSimulator) Finished() bool {
	return g.madePoints >= g.maxPoints
}

// Type DevopsSimulatorConfig is used to create a DevopsSimulator.
type DevopsSimulatorConfig struct {
	Start time.Time
	End   time.Time

	HostCount       int64
	HostConstructor func(i int, start time.Time) Host
}

func (d *DevopsSimulatorConfig) ToSimulator() *DevopsSimulator {
	hostInfos := make([]Host, d.HostCount)
	for i := 0; i < len(hostInfos); i++ {
		hostInfos[i] = d.HostConstructor(i, d.Start)
	}

	epochs := d.End.Sub(d.Start).Nanoseconds() / EpochDuration.Nanoseconds()
	maxPoints := epochs * (d.HostCount * int64(len(hostInfos[0].SimulatedMeasurements)))
	dg := &DevopsSimulator{
		madePoints: 0,
		maxPoints:  maxPoints,

		simulatedMeasurementIndex: 0,

		hostIndex: 0,
		hosts:     hostInfos,

		timestampNow:   d.Start,
		timestampStart: d.Start,
		timestampEnd:   d.End,
	}

	return dg
}

func (d *DevopsSimulator) Fields() map[string][][]byte {
	data := make(map[string][][]byte)
	for _, sm := range d.hosts[0].SimulatedMeasurements {
		point := MakeUsablePoint()
		sm.ToPoint(point)
		data[string(point.MeasurementName)] = point.FieldKeys
	}

	return data
}

// Next advances a Point to the next state in the generator.
func (d *DevopsSimulator) Next(p *Point) {
	// switch to the next metric if needed
	if d.hostIndex == len(d.hosts) {
		d.hostIndex = 0
		d.simulatedMeasurementIndex++
	}

	if d.simulatedMeasurementIndex == len(d.hosts[0].SimulatedMeasurements) {
		d.simulatedMeasurementIndex = 0

		for i := 0; i < len(d.hosts); i++ {
			d.hosts[i].TickAll(EpochDuration)
		}
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

	d.madePoints++
	d.hostIndex++

	return
}
