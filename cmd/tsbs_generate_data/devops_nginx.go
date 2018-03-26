package main

import (
	"fmt"
	"math/rand"
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/cmd/tsbs_generate_data/serialize"
)

var (
	NginxByteString = []byte("nginx") // heap optimization

	NginxTags = [][]byte{
		[]byte("port"),
		[]byte("server"),
	}

	NginxFields = []LabeledDistributionMaker{
		{[]byte("accepts"), func() Distribution { return MWD(ND(5, 1), 0) }},
		{[]byte("active"), func() Distribution { return CWD(ND(5, 1), 0, 100, 0) }},
		{[]byte("handled"), func() Distribution { return MWD(ND(5, 1), 0) }},
		{[]byte("reading"), func() Distribution { return CWD(ND(5, 1), 0, 100, 0) }},
		{[]byte("requests"), func() Distribution { return MWD(ND(5, 1), 0) }},
		{[]byte("waiting"), func() Distribution { return CWD(ND(5, 1), 0, 100, 0) }},
		{[]byte("writing"), func() Distribution { return CWD(ND(5, 1), 0, 100, 0) }},
	}
)

type NginxMeasurement struct {
	timestamp time.Time

	port, serverName []byte
	distributions    []Distribution
}

func NewNginxMeasurement(start time.Time) *NginxMeasurement {
	distributions := make([]Distribution, len(NginxFields))
	for i := range NginxFields {
		distributions[i] = NginxFields[i].DistributionMaker()
	}

	serverName := []byte(fmt.Sprintf("nginx_%d", rand.Intn(100000)))
	port := []byte(fmt.Sprintf("%d", rand.Intn(20000)+1024))
	return &NginxMeasurement{
		port:       port,
		serverName: serverName,

		timestamp:     start,
		distributions: distributions,
	}
}

func (m *NginxMeasurement) Tick(d time.Duration) {
	m.timestamp = m.timestamp.Add(d)

	for i := range m.distributions {
		m.distributions[i].Advance()
	}
}

func (m *NginxMeasurement) ToPoint(p *serialize.Point) {
	p.SetMeasurementName(NginxByteString)
	p.SetTimestamp(&m.timestamp)

	p.AppendTag(NginxTags[0], m.port)
	p.AppendTag(NginxTags[1], m.serverName)

	for i := range m.distributions {
		p.AppendField(NginxFields[i].Label, int64(m.distributions[i].Get()))
	}
}
