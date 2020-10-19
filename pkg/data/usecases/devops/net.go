package devops

import (
	"fmt"
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"math/rand"
	"time"
)

var (
	labelNet             = []byte("net") // heap optimization
	labelNetTagInterface = []byte("interface")

	// Reuse NormalDistributions as arguments to other distributions. This is
	// safe to do because the higher-level distribution advances the ND and
	// immediately uses its value and saves the state
	highND = common.ND(50, 1)
	lowND  = common.ND(5, 1)

	netFields = []common.LabeledDistributionMaker{
		{Label: []byte("bytes_sent"), DistributionMaker: func() common.Distribution { return common.MWD(highND, 0) }},
		{Label: []byte("bytes_recv"), DistributionMaker: func() common.Distribution { return common.MWD(highND, 0) }},
		{Label: []byte("packets_sent"), DistributionMaker: func() common.Distribution { return common.MWD(highND, 0) }},
		{Label: []byte("packets_recv"), DistributionMaker: func() common.Distribution { return common.MWD(highND, 0) }},
		{Label: []byte("err_in"), DistributionMaker: func() common.Distribution { return common.MWD(lowND, 0) }},
		{Label: []byte("err_out"), DistributionMaker: func() common.Distribution { return common.MWD(lowND, 0) }},
		{Label: []byte("drop_in"), DistributionMaker: func() common.Distribution { return common.MWD(lowND, 0) }},
		{Label: []byte("drop_out"), DistributionMaker: func() common.Distribution { return common.MWD(lowND, 0) }},
	}
)

type NetMeasurement struct {
	*common.SubsystemMeasurement
	interfaceName string
}

func NewNetMeasurement(start time.Time) *NetMeasurement {
	sub := common.NewSubsystemMeasurementWithDistributionMakers(start, netFields)
	interfaceName := fmt.Sprintf("eth%d", rand.Intn(4))
	return &NetMeasurement{
		SubsystemMeasurement: sub,
		interfaceName:        interfaceName,
	}
}

func (m *NetMeasurement) ToPoint(p *data.Point) {
	m.ToPointAllInt64(p, labelNet, netFields)
	p.AppendTag(labelNetTagInterface, m.interfaceName)
}
