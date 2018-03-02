package main

import (
	"time"
)

// A CPUOnlySimulator generates data similar to telemetry from Telegraf for only CPU metrics.
// It fulfills the Simulator interface.
type CPUOnlySimulator struct {
	madePoints int64
	maxPoints  int64

	hostIndex int
	hosts     []Host

	timestampNow   time.Time
	timestampStart time.Time
	timestampEnd   time.Time
	interval       time.Duration
}

func (d *CPUOnlySimulator) Seen() int64 {
	return d.madePoints
}

func (d *CPUOnlySimulator) Total() int64 {
	return d.maxPoints
}

func (d *CPUOnlySimulator) Finished() bool {
	return d.madePoints >= d.maxPoints
}

// CPUOnlySimulatorConfig is used to create a CPUOnlySimulator.
type CPUOnlySimulatorConfig struct {
	Start time.Time
	End   time.Time

	HostCount       int64
	HostConstructor func(i int, start time.Time) Host
}

func (d *CPUOnlySimulatorConfig) ToSimulator(interval time.Duration) Simulator {
	hostInfos := make([]Host, d.HostCount)
	for i := 0; i < len(hostInfos); i++ {
		hostInfos[i] = d.HostConstructor(i, d.Start)
	}

	epochs := d.End.Sub(d.Start).Nanoseconds() / interval.Nanoseconds()
	maxPoints := epochs * d.HostCount
	dg := &CPUOnlySimulator{
		madePoints: 0,
		maxPoints:  maxPoints,

		hostIndex: 0,
		hosts:     hostInfos,

		timestampNow:   d.Start,
		timestampStart: d.Start,
		timestampEnd:   d.End,
		interval:       interval,
	}

	return dg
}

func (d *CPUOnlySimulator) Fields() map[string][][]byte {
	data := make(map[string][][]byte)
	point := MakeUsablePoint()
	d.hosts[0].SimulatedMeasurements[0].ToPoint(point)
	data[string(point.MeasurementName)] = point.FieldKeys

	return data
}

// Next advances a Point to the next state in the generator.
func (d *CPUOnlySimulator) Next(p *Point) {
	// switch to the next metric if needed
	if d.hostIndex == len(d.hosts) {
		d.hostIndex = 0

		for i := 0; i < len(d.hosts); i++ {
			d.hosts[i].TickAll(d.interval)
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
	host.SimulatedMeasurements[0].ToPoint(p)

	d.madePoints++
	d.hostIndex++

	return
}
