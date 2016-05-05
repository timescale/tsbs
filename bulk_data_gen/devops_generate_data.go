package main

import (
	"time"
)

// A DevopsGenerator generates data similar to the Telegraf CPU and Memory
// measurements. It is format-agnostic.
type DevopsGenerator struct {
	madePoints int64
	maxPoints  int64

	simulatedMeasurementIndex int

	hostIndex int
	hosts     []Host

	timestampNow   time.Time
	timestampStart time.Time
	timestampEnd   time.Time
}

func (g *DevopsGenerator) Seen() int64 {
	return g.madePoints
}

func (g *DevopsGenerator) Total() int64 {
	return g.maxPoints
}

func (g *DevopsGenerator) Finished() bool {
	return g.madePoints >= g.maxPoints
}

// Type DevopsGeneratorConfig is used to create a DevopsGenerator.
type DevopsGeneratorConfig struct {
	Start time.Time
	End   time.Time

	HostCount int64
}

func (d *DevopsGeneratorConfig) ToMeasurementGenerator() *DevopsGenerator {
	hostInfos := make([]Host, d.HostCount)
	for i := 0; i < len(hostInfos); i++ {
		hostInfos[i] = NewHost(i, d.Start)
	}

	epochs := d.End.Sub(d.Start).Nanoseconds() / EpochDuration.Nanoseconds()
	maxPoints := epochs * (d.HostCount * 2)
	dg := &DevopsGenerator{
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

// MakeUsablePoint allocates a new Point ready for use by a DevopsGenerator.
func (d *DevopsGenerator) MakeUsablePoint() *Point {
	return &Point{
		MeasurementName: nil,
		TagKeys:         make([][]byte, 0),
		TagValues:       make([][]byte, 0),
		FieldKeys:       make([][]byte, 0),
		FieldValues:     make([]interface{}, 0),
		Timestamp:       &time.Time{},
	}
}

// Next advances a Point to the next state in the generator.
func (d *DevopsGenerator) Next(p *Point) {
	// switch to the next metric if needed
	if d.hostIndex == len(d.hosts) {
		d.hostIndex = 0
		d.simulatedMeasurementIndex++
	}

	if d.simulatedMeasurementIndex == NHostSims {
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

	host.SimulatedMeasurements[d.simulatedMeasurementIndex].ToPoint(p)

	d.madePoints++
	d.hostIndex++

	return
}
