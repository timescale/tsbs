package devops

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_data/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_data/serialize"
)

var (
	labelNginx          = []byte("nginx") // heap optimization
	labelNginxTagPort   = []byte("port")
	labelNginxTagServer = []byte("server")

	// Reuse NormalDistributions as arguments to other distributions. This is
	// safe to do because the higher-level distribution advances the ND and
	// immediately uses its value and saves the state
	nginxND = common.ND(5, 1)

	nginxFields = []labeledDistributionMaker{
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
	sub := newSubsystemMeasurementWithDistributionMakers(start, nginxFields)
	serverName := []byte(fmt.Sprintf("nginx_%d", rand.Intn(100000)))
	port := []byte(fmt.Sprintf("%d", rand.Intn(20000)+1024))
	return &NginxMeasurement{
		subsystemMeasurement: sub,
		port:                 port,
		serverName:           serverName,
	}
}

func (m *NginxMeasurement) ToPoint(p *serialize.Point) {
	m.toPointAllInt64(p, labelNginx, nginxFields)
	p.AppendTag(labelNginxTagPort, m.port)
	p.AppendTag(labelNginxTagServer, m.serverName)
}
