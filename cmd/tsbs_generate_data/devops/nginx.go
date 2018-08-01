package devops

import (
	"fmt"
	"math/rand"
	"time"

	"bitbucket.org/440-labs/tsbs/cmd/tsbs_generate_data/common"
	"bitbucket.org/440-labs/tsbs/cmd/tsbs_generate_data/serialize"
)

var (
	NginxByteString = []byte("nginx") // heap optimization

	NginxTags = [][]byte{
		[]byte("port"),
		[]byte("server"),
	}

	// Reuse NormalDistributions as arguments to other distributions. This is
	// safe to do because the higher-level distribution advances the ND and
	// immediately uses its value and saves the state
	nginxND = common.ND(5, 1)

	NginxFields = []labeledDistributionMaker{
		{[]byte("accepts"), func() common.Distribution { return common.MWD(nginxND, 0) }},
		{[]byte("active"), func() common.Distribution { return common.CWD(nginxND, 0, 100, 0) }},
		{[]byte("handled"), func() common.Distribution { return common.MWD(nginxND, 0) }},
		{[]byte("reading"), func() common.Distribution { return common.CWD(nginxND, 0, 100, 0) }},
		{[]byte("requests"), func() common.Distribution { return common.MWD(nginxND, 0) }},
		{[]byte("waiting"), func() common.Distribution { return common.CWD(nginxND, 0, 100, 0) }},
		{[]byte("writing"), func() common.Distribution { return common.CWD(nginxND, 0, 100, 0) }},
	}
)

type NginxMeasurement struct {
	*subsystemMeasurement
	port, serverName []byte
}

func NewNginxMeasurement(start time.Time) *NginxMeasurement {
	sub := newSubsystemMeasurement(start, len(NginxFields))
	for i := range NginxFields {
		sub.distributions[i] = NginxFields[i].distributionMaker()
	}

	serverName := []byte(fmt.Sprintf("nginx_%d", rand.Intn(100000)))
	port := []byte(fmt.Sprintf("%d", rand.Intn(20000)+1024))
	return &NginxMeasurement{
		subsystemMeasurement: sub,
		port:                 port,
		serverName:           serverName,
	}
}

func (m *NginxMeasurement) ToPoint(p *serialize.Point) {
	p.SetMeasurementName(NginxByteString)
	p.SetTimestamp(&m.timestamp)

	p.AppendTag(NginxTags[0], m.port)
	p.AppendTag(NginxTags[1], m.serverName)

	for i := range m.distributions {
		p.AppendField(NginxFields[i].label, int64(m.distributions[i].Get()))
	}
}
