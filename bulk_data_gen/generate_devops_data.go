package main

import (
	"fmt"
	"math"
	"math/rand"
	"time"
)

// Type Host models a machine being monitored by Telegraf. Its Name looks like
// "host_123", the Datacenter is randomly chosen from MachineDatacenters,
// and the BytesTotal is chosen from MemoryMaxBytesChoices.
//
// It models a host through time by using stateful distributions for memory
// and CPU usage.
type Host struct {
	// These are all assigned once, at Host creation:
	Name, Region, Datacenter, Rack, OS, Arch          []byte
	Team, Service, ServiceVersion, ServiceEnvironment []byte
	BytesTotal                                        int64

	// These are updated each epoch:
	BytesUsed, BytesCached, BytesBuffered Distribution
	CPUFieldDistributions                 []Distribution
}

// NewHost creates a Host instance populated with randomly-generated data
// representing a machine's status for both CPU and Memory measurements.
func NewHost(i int) Host {
	bytesTotal := MemoryMaxBytesChoices[rand.Intn(len(MemoryMaxBytesChoices))]
	region := &Regions[rand.Intn(len(Regions))]
	rackId := rand.Int63n(MachineRackChoicesPerDatacenter)
	serviceId := rand.Int63n(MachineServiceChoices)
	serviceVersionId := rand.Int63n(MachineServiceVersionChoices)
	serviceEnvironment := randChoice(MachineServiceEnvironmentChoices)

	h := Host{
		// Tag Values that are static throughout the life of a Host:
		Name:               []byte(fmt.Sprintf("host_%d", i)),
		Region:             []byte(fmt.Sprintf("%s", region.Name)),
		Datacenter:         randChoice(region.Datacenters),
		Rack:               []byte(fmt.Sprintf("%d", rackId)),
		Arch:               randChoice(MachineArchChoices),
		OS:                 randChoice(MachineOSChoices),
		Service:            []byte(fmt.Sprintf("%d", serviceId)),
		ServiceVersion:     []byte(fmt.Sprintf("%d", serviceVersionId)),
		ServiceEnvironment: serviceEnvironment,
		Team:               randChoice(MachineTeamChoices),

		// CPU models (updated each epoch):
		CPUFieldDistributions: newCPUDistributions(len(CPUFieldKeys)),

		// Memory models (updated each epoch):
		BytesTotal: bytesTotal,
		BytesUsed: &ClampedRandomWalkDistribution{
			State: rand.Float64() * float64(bytesTotal),
			Min:   0.0,
			Max:   float64(bytesTotal),
			Step: &NormalDistribution{
				Mean:   0.0,
				StdDev: float64(bytesTotal) / 64,
			},
		},
		BytesCached: &ClampedRandomWalkDistribution{
			State: rand.Float64() * float64(bytesTotal),
			Min:   0.0,
			Max:   float64(bytesTotal),
			Step: &NormalDistribution{
				Mean:   0.0,
				StdDev: float64(bytesTotal) / 64,
			},
		},
		BytesBuffered: &ClampedRandomWalkDistribution{
			State: rand.Float64() * float64(bytesTotal),
			Min:   0.0,
			Max:   float64(bytesTotal),
			Step: &NormalDistribution{
				Mean:   0.0,
				StdDev: float64(bytesTotal) / 64,
			},
		},
	}

	// initialize all the distributions:
	h.AdvanceAll()

	return h
}

// AdvanceAll advances all Distributions of a Host.
func (h *Host) AdvanceAll() {
	for i := 0; i < len(h.CPUFieldDistributions); i++ {
		h.CPUFieldDistributions[i].Advance()
	}
	h.BytesUsed.Advance()
	h.BytesCached.Advance()
	h.BytesBuffered.Advance()
}

// newCPUDistributions creates fresh Distributions for a Host CPU measurement.
func newCPUDistributions(count int) []Distribution {
	dd := make([]Distribution, count)
	for i := 0; i < len(dd); i++ {
		dd[i] = &ClampedRandomWalkDistribution{
			State: rand.Float64() * 100.0,
			Min:   0.0,
			Max:   100.0,
			Step: &NormalDistribution{
				Mean:   0.0,
				StdDev: 1.0,
			},
		}
	}
	return dd
}

// A DevopsGenerator generates data similar to the Telegraf CPU and Memory
// measurements. It is format-agnostic.
type DevopsGenerator struct {
	madePoints int64
	maxPoints  int64

	enumerationMode int

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
		hostInfos[i] = NewHost(i)
	}

	epochs := d.End.Sub(d.Start).Nanoseconds() / EpochDuration.Nanoseconds()
	maxPoints := epochs * (d.HostCount * 2)
	dg := &DevopsGenerator{
		madePoints: 0,
		maxPoints:  maxPoints,

		hostIndex: 0,
		hosts:     hostInfos,

		enumerationMode: enumerationModeCPU,

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
		Timestamp:       time.Time{},
	}
}

// Next advances a Point to the next state in the generator.
func (d *DevopsGenerator) Next(p *Point) {
	// switch to the next metric if needed
	if d.hostIndex == len(d.hosts) {
		d.hostIndex = 0

		switch d.enumerationMode {
		case enumerationModeCPU:
			d.enumerationMode = enumerationModeMem
		case enumerationModeMem:
			d.enumerationMode = enumerationModeCPU

			// Update the timestamp (applies to all points in this epoch):
			d.timestampNow = d.timestampNow.Add(EpochDuration)

			// Update the generators on each Host:
			for i := 0; i < len(d.hosts); i++ {
				d.hosts[i].AdvanceAll()
			}
		default:
			panic("unreachable")
		}
	}

	host := &d.hosts[d.hostIndex]

	// Populate timestamp:
	p.Timestamp = d.timestampNow

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

	switch d.enumerationMode {
	case enumerationModeCPU:
		// Populate CPU-specific labels:
		p.MeasurementName = CPUByteString

		p.AppendTag(CPUByteString, CPUTotalByteString)

		// Populate CPU-specific data:
		for i := 0; i < len(CPUFieldKeys); i++ {
			n := host.CPUFieldDistributions[i].Get()
			p.AppendField(CPUFieldKeys[i], n)
		}

	case enumerationModeMem:
		// Populate Mem-specific labels:
		p.MeasurementName = MemoryByteString

		// Populate Memory-specific data:
		p.AppendField(MemoryFieldKeys[0], host.BytesTotal)
		p.AppendField(MemoryFieldKeys[1], int(math.Floor(float64(host.BytesTotal)-host.BytesUsed.Get())))
		p.AppendField(MemoryFieldKeys[2], int(math.Floor(host.BytesUsed.Get())))
		p.AppendField(MemoryFieldKeys[3], int(math.Floor(host.BytesCached.Get())))
		p.AppendField(MemoryFieldKeys[4], int(math.Floor(host.BytesBuffered.Get())))
		p.AppendField(MemoryFieldKeys[5], int(math.Floor(host.BytesUsed.Get())))
		p.AppendField(MemoryFieldKeys[6], 100.0*(host.BytesUsed.Get()/float64(host.BytesTotal)))
		p.AppendField(MemoryFieldKeys[7], 100.0*(float64(host.BytesTotal)-host.BytesUsed.Get())/float64(host.BytesTotal))
	default:
		panic("unreachable")
	}

	d.madePoints++
	d.hostIndex++

	return
}

func randChoice(choices [][]byte) []byte {
	idx := rand.Int63n(int64(len(choices)))
	return choices[idx]
}
