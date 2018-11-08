package devops

import (
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_data/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_data/serialize"
)

// DevopsSimulator generates data similar to telemetry, with metrics from a variety of device systems.
// It fulfills the Simulator interface.
type DevopsSimulator struct {
	*commonDevopsSimulator
	simulatedMeasurementIndex int
}

// Next advances a Point to the next state in the generator.
func (d *DevopsSimulator) Next(p *serialize.Point) bool {
	// switch to the next metric if needed
	if d.hostIndex == uint64(len(d.hosts)) {
		d.hostIndex = 0
		d.simulatedMeasurementIndex++
	}

	if d.simulatedMeasurementIndex == len(d.hosts[0].SimulatedMeasurements) {
		d.simulatedMeasurementIndex = 0

		for i := 0; i < len(d.hosts); i++ {
			d.hosts[i].TickAll(d.interval)
		}

		d.adjustNumHostsForEpoch()
	}

	return d.populatePoint(p, d.simulatedMeasurementIndex)
}

// DevopsSimulatorConfig is used to create a DevopsSimulator.
type DevopsSimulatorConfig commonDevopsSimulatorConfig

// NewSimulator produces a Simulator that conforms to the given SimulatorConfig over the specified interval
func (d *DevopsSimulatorConfig) NewSimulator(interval time.Duration, limit uint64) common.Simulator {
	hostInfos := make([]Host, d.HostCount)
	for i := 0; i < len(hostInfos); i++ {
		hostInfos[i] = d.HostConstructor(i, d.Start)
	}

	epochs := calculateEpochs(commonDevopsSimulatorConfig(*d), interval)
	maxPoints := epochs * d.HostCount * uint64(len(hostInfos[0].SimulatedMeasurements))
	if limit > 0 && limit < maxPoints {
		// Set specified points number limit
		maxPoints = limit
	}
	dg := &DevopsSimulator{
		commonDevopsSimulator: &commonDevopsSimulator{
			madePoints: 0,
			maxPoints:  maxPoints,

			hostIndex: 0,
			hosts:     hostInfos,

			epoch:          0,
			epochs:         epochs,
			epochHosts:     d.InitHostCount,
			initHosts:      d.InitHostCount,
			timestampStart: d.Start,
			timestampEnd:   d.End,
			interval:       interval,
		},
		simulatedMeasurementIndex: 0,
	}

	return dg
}
