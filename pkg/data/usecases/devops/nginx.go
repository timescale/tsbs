package devops

import (
	"fmt"
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"math/rand"
	"strconv"
	"time"
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
		{Label: []byte("accepts"), DistributionMaker: func() common.Distribution { return common.MWD(nginxND, 0) }},
		{Label: []byte("active"), DistributionMaker: func() common.Distribution { return common.CWD(nginxND, 0, 100, 0) }},
		{Label: []byte("handled"), DistributionMaker: func() common.Distribution { return common.MWD(nginxND, 0) }},
		{Label: []byte("reading"), DistributionMaker: func() common.Distribution { return common.CWD(nginxND, 0, 100, 0) }},
		{Label: []byte("requests"), DistributionMaker: func() common.Distribution { return common.MWD(nginxND, 0) }},
		{Label: []byte("waiting"), DistributionMaker: func() common.Distribution { return common.CWD(nginxND, 0, 100, 0) }},
		{Label: []byte("writing"), DistributionMaker: func() common.Distribution { return common.CWD(nginxND, 0, 100, 0) }},
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

func (m *NginxMeasurement) ToPoint(p *data.Point) {
	m.ToPointAllInt64(p, labelNginx, nginxFields)
	p.AppendTag(labelNginxTagPort, m.port)
	p.AppendTag(labelNginxTagServer, m.serverName)
}
