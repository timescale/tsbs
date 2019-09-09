package devops

import (
	"fmt"
	"math/rand"
	"strconv"
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

	nginxFields = []common.LabeledDistributionMaker{
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
	*common.SubsystemMeasurement
	port, serverName string
}

func NewNginxMeasurement(start time.Time) *NginxMeasurement {
	sub := common.NewSubsystemMeasurementWithDistributionMakers(start, nginxFields)
	serverName := fmt.Sprintf("nginx_%d", rand.Intn(100000))
	port := strconv.FormatInt(rand.Int63n(20000)+1024, 10)
	return &NginxMeasurement{
		SubsystemMeasurement: sub,
		port:                 port,
		serverName:           serverName,
	}
}

func (m *NginxMeasurement) ToPoint(p *serialize.Point) {
	m.ToPointAllInt64(p, labelNginx, nginxFields)
	p.AppendTag(labelNginxTagPort, m.port)
	p.AppendTag(labelNginxTagServer, m.serverName)
}
