package main

import (
	"fmt"
	"math"
	"math/rand"
	"time"
)

const (
	enumerationModeCPU = iota
	enumerationModeMem
)

var (
	CPUByteString      = []byte("cpu")       // heap optimization
	CPUTotalByteString = []byte("cpu-total") // heap optimization
	MemoryByteString   = []byte("mem")       // heap optimization
)

var (
	MemoryMaxBytesChoices = []int64{8 << 30, 12 << 30, 16 << 30}

	MachineTagKeys = [][]byte{
		[]byte("cpu"),
		[]byte("host"),
		[]byte("dc"),
	}

	MachineDatacenters = [][]byte{
		[]byte("us-east-1"),
		[]byte("us-west-2"),
		[]byte("us-west-1"),
		[]byte("eu-west-1"),
		[]byte("eu-central-1"),
		[]byte("ap-southeast-1"),
		[]byte("ap-northeast-1"),
		[]byte("ap-southeast-2"),
		[]byte("ap-northeast-2"),
		[]byte("sa-east-1"),
	}

	CPUFieldKeys = [][]byte{
		[]byte("usage_user"),
		[]byte("usage_system"),
		[]byte("usage_idle"),
		[]byte("usage_nice"),
		[]byte("usage_iowait"),
		[]byte("usage_irq"),
		[]byte("usage_softirq"),
		[]byte("usage_steal"),
		[]byte("usage_guest"),
		[]byte("usage_guest_nice"),
	}

	MemoryFieldKeys = [][]byte{
		[]byte("total"),
		[]byte("available"),
		[]byte("used"),
		[]byte("free"),
		[]byte("cached"),
		[]byte("buffered"),
		[]byte("used_percent"),
		[]byte("available_percent"),
	}
)

type Host struct {
	Name, Datacenter []byte

	BytesTotal                            int64
	BytesUsed, BytesCached, BytesBuffered Distribution

	CPUFieldDistributions []Distribution
}

// NewHost creates a Host instance populated with randomly-generated data
// representing a machine's status for both CPU and Memory measurements.
func NewHost(i int) Host {
	bytesTotal := MemoryMaxBytesChoices[rand.Intn(len(MemoryMaxBytesChoices))]
	return Host{
		Name:                  []byte(fmt.Sprintf("host_%d", i)),
		Datacenter:            randChoice(MachineDatacenters),
		CPUFieldDistributions: newCPUDistributions(len(CPUFieldKeys)),
		BytesTotal:            bytesTotal,
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

// Type DevopsGenerator generates data similar to the Telegraf CPU and Memory
// measurements.
type DevopsGenerator struct {
	point      *Point
	madePoints int64
	maxPoints  int64

	enumerationMode int

	hostIndex int
	hosts     []Host

	timestampNow       time.Time
	timestampStart     time.Time
	timestampIncrement time.Duration
	timestampEnd       time.Time
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

	timestampIncrement := 10 * time.Second
	epochs := d.End.Sub(d.Start).Nanoseconds() / timestampIncrement.Nanoseconds()
	maxPoints := epochs * (d.HostCount * 2)
	dg := &DevopsGenerator{
		point: nil,

		madePoints: 0,
		maxPoints:  maxPoints,

		hostIndex: 0,
		hosts:     hostInfos,

		enumerationMode: enumerationModeCPU,

		timestampNow:       d.Start,
		timestampStart:     d.Start,
		timestampIncrement: timestampIncrement,
		timestampEnd:       d.End,
	}

	return dg
}

func (d *DevopsGenerator) makeUsablePoint() *Point {
	neededFieldKeys := len(CPUFieldKeys)
	if neededFieldKeys < len(MemoryFieldKeys) {
		neededFieldKeys = len(MemoryFieldKeys)
	}
	return &Point{
		MeasurementName: nil,
		TagKeys:         nil,
		TagValues:       make([][]byte, 0, len(MachineTagKeys)),
		FieldKeys:       nil,
		FieldValues:     make([]interface{}, 0, neededFieldKeys),
		Timestamp:       time.Time{},
	}
}

func (d *DevopsGenerator) Next() *Point {
	if d.point == nil {
		d.point = d.makeUsablePoint()
	}

	// switch to the next metric if needed
	if d.hostIndex == len(d.hosts) {
		d.hostIndex = 0

		// Update the timestamp (applies to all points in this epoch):
		d.timestampNow = d.timestampNow.Add(d.timestampIncrement)

		// Update the generators on each Host:
		for i := 0; i < len(d.hosts); i++ {
			d.hosts[i].AdvanceAll()
		}

		switch d.enumerationMode {
		case enumerationModeCPU:
			d.enumerationMode = enumerationModeMem
		case enumerationModeMem:
			d.enumerationMode = enumerationModeCPU
		default:
			panic("unreachable")
		}
	}

	host := &d.hosts[d.hostIndex]
	p := d.point

	// Populate the data that apply to both CPU and Mem points:
	p.TagKeys = MachineTagKeys
	p.TagValues = p.TagValues[:len(MachineTagKeys)]
	p.TagValues[0] = CPUTotalByteString
	p.TagValues[1] = host.Name
	p.TagValues[2] = host.Datacenter
	p.Timestamp = d.timestampNow

	switch d.enumerationMode {
	case enumerationModeCPU:
		// Populate CPU-specific labels:
		p.MeasurementName = CPUByteString
		p.FieldKeys = CPUFieldKeys

		// Ensure correct len:
		p.FieldValues = p.FieldValues[:len(host.CPUFieldDistributions)]

		// Populate CPU-specific data:
		for i := 0; i < len(p.FieldValues); i++ {
			n := host.CPUFieldDistributions[i].Get()
			p.FieldValues[i] = n //[]byte(fmt.Sprintf("%.2f", n))
		}

	case enumerationModeMem:
		// Populate Mem-specific labels:
		p.MeasurementName = MemoryByteString
		p.FieldKeys = MemoryFieldKeys

		// Ensure correct len:
		p.FieldValues = p.FieldValues[:len(MemoryFieldKeys)]

		// Populate Memory-specific data:
		//p.FieldValues[0] = []byte(fmt.Sprintf("%d", host.BytesTotal))
		//p.FieldValues[1] = []byte(fmt.Sprintf("%d", int(math.Floor(float64(host.BytesTotal)-host.BytesUsed.Get()))))
		//p.FieldValues[2] = []byte(fmt.Sprintf("%d", int(math.Floor(host.BytesUsed.Get()))))
		//p.FieldValues[3] = []byte(fmt.Sprintf("%d", int(math.Floor(host.BytesCached.Get()))))
		//p.FieldValues[4] = []byte(fmt.Sprintf("%d", int(math.Floor(host.BytesBuffered.Get()))))
		//p.FieldValues[5] = []byte(fmt.Sprintf("%d", int(math.Floor(host.BytesUsed.Get()))))
		//p.FieldValues[6] = []byte(fmt.Sprintf("%.2f", 100.0*(host.BytesUsed.Get()/float64(host.BytesTotal))))
		//p.FieldValues[7] = []byte(fmt.Sprintf("%.2f", 100.0*(float64(host.BytesTotal)-host.BytesUsed.Get())/float64(host.BytesTotal)))
		p.FieldValues[0] = host.BytesTotal
		p.FieldValues[1] = int(math.Floor(float64(host.BytesTotal) - host.BytesUsed.Get()))
		p.FieldValues[2] = int(math.Floor(host.BytesUsed.Get()))
		p.FieldValues[3] = int(math.Floor(host.BytesCached.Get()))
		p.FieldValues[4] = int(math.Floor(host.BytesBuffered.Get()))
		p.FieldValues[5] = int(math.Floor(host.BytesUsed.Get()))
		p.FieldValues[6] = 100.0 * (host.BytesUsed.Get() / float64(host.BytesTotal))
		p.FieldValues[7] = 100.0 * (float64(host.BytesTotal) - host.BytesUsed.Get()) / float64(host.BytesTotal)
	default:
		panic("unreachable")
	}

	d.madePoints++
	d.hostIndex++

	return p
}

func randChoice(choices [][]byte) []byte {
	idx := rand.Int63n(int64(len(choices)))
	return choices[idx]
}
