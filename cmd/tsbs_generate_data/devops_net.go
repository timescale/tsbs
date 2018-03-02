package main

import (
	"fmt"
	"math/rand"
	"time"
)

var (
	NetByteString = []byte("net") // heap optimization
	NetTags       = [][]byte{
		[]byte("interface"),
	}

	NetFields = []LabeledDistributionMaker{
		{[]byte("bytes_sent"), func() Distribution { return MWD(ND(50, 1), 0) }},
		{[]byte("bytes_recv"), func() Distribution { return MWD(ND(50, 1), 0) }},
		{[]byte("packets_sent"), func() Distribution { return MWD(ND(50, 1), 0) }},
		{[]byte("packets_recv"), func() Distribution { return MWD(ND(50, 1), 0) }},
		{[]byte("err_in"), func() Distribution { return MWD(ND(5, 1), 0) }},
		{[]byte("err_out"), func() Distribution { return MWD(ND(5, 1), 0) }},
		{[]byte("drop_in"), func() Distribution { return MWD(ND(5, 1), 0) }},
		{[]byte("drop_out"), func() Distribution { return MWD(ND(5, 1), 0) }},
	}
)

type NetMeasurement struct {
	timestamp time.Time

	interfaceName []byte
	uptime        time.Duration
	distributions []Distribution
}

func NewNetMeasurement(start time.Time) *NetMeasurement {
	distributions := make([]Distribution, len(NetFields))
	for i := range NetFields {
		distributions[i] = NetFields[i].DistributionMaker()
	}

	interfaceName := []byte(fmt.Sprintf("eth%d", rand.Intn(4)))
	return &NetMeasurement{
		interfaceName: interfaceName,

		timestamp:     start,
		distributions: distributions,
	}
}

func (m *NetMeasurement) Tick(d time.Duration) {
	m.timestamp = m.timestamp.Add(d)

	for i := range m.distributions {
		m.distributions[i].Advance()
	}
}

func (m *NetMeasurement) ToPoint(p *Point) {
	p.SetMeasurementName(NetByteString)
	p.SetTimestamp(&m.timestamp)

	p.AppendTag(NetTags[0], m.interfaceName)

	for i := range m.distributions {
		p.AppendField(RedisFields[i].Label, int64(m.distributions[i].Get()))
	}
}
