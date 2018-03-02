package main

import (
	"fmt"
	"math/rand"
	"time"
)

var (
	DiskIOByteString = []byte("diskio") // heap optimization
	SerialByteString = []byte("serial")

	DiskIOFields = []LabeledDistributionMaker{
		{[]byte("reads"), func() Distribution { return MWD(ND(50, 1), 0) }},
		{[]byte("writes"), func() Distribution { return MWD(ND(50, 1), 0) }},
		{[]byte("read_bytes"), func() Distribution { return MWD(ND(100, 1), 0) }},
		{[]byte("write_bytes"), func() Distribution { return MWD(ND(100, 1), 0) }},
		{[]byte("read_time"), func() Distribution { return MWD(ND(5, 1), 0) }},
		{[]byte("write_time"), func() Distribution { return MWD(ND(5, 1), 0) }},
		{[]byte("io_time"), func() Distribution { return MWD(ND(5, 1), 0) }},
	}
)

type DiskIOMeasurement struct {
	timestamp time.Time

	serial []byte
	distributions    []Distribution
}

func NewDiskIOMeasurement(start time.Time) *DiskIOMeasurement {
	distributions := make([]Distribution, len(DiskIOFields))
	for i := range DiskIOFields {
		distributions[i] = DiskIOFields[i].DistributionMaker()
	}

	serial := []byte(fmt.Sprintf("%03d-%03d-%03d", rand.Intn(1000), rand.Intn(1000), rand.Intn(1000)))
	return &DiskIOMeasurement{
		serial:       serial,

		timestamp:     start,
		distributions: distributions,
	}
}

func (m *DiskIOMeasurement) Tick(d time.Duration) {
	m.timestamp = m.timestamp.Add(d)

	for i := range m.distributions {
		m.distributions[i].Advance()
	}
}

func (m *DiskIOMeasurement) ToPoint(p *Point) {
	p.SetMeasurementName(DiskIOByteString)
	p.SetTimestamp(&m.timestamp)

	p.AppendTag(SerialByteString, m.serial)

	for i := range m.distributions {
		p.AppendField(DiskIOFields[i].Label, int64(m.distributions[i].Get()))
	}
}
