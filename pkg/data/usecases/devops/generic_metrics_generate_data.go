package devops

import (
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"math"
	"time"
)

type GenericMetricsSimulatorConfig struct {
	*DevopsSimulatorConfig
}

// GenericMetricsSimulator provides a possibility to generate a configurable amount of metrics per host where
// other simulators have hardcoded metric set. This simulator also controls how long a host lives (hosts lifetime
// follows zipf distribution)
type GenericMetricsSimulator struct {
	*commonDevopsSimulator
}

// NewSimulator creates GenericMetricsSimulator for generic-devops use-case. Number of metrics assigned to each host follow zipf distribution.
// 50% of hosts is long lived and 50% has a liftspan that follows zipf distribution.
func (c *GenericMetricsSimulatorConfig) NewSimulator(interval time.Duration, limit uint64) common.Simulator {
	hostInfos := make([]Host, c.HostCount)
	// initialize all generic metric fields at once so they can be reused for different hosts
	initGenericMetricFields(c.MaxMetricCount)
	hostMetricCount := generateHostMetricCount(c.HostCount, c.MaxMetricCount)
	epochs := calculateEpochs(commonDevopsSimulatorConfig(*c.DevopsSimulatorConfig), interval)
	epochsToLive := generateHostEpochsToLive(c.HostCount, epochs)
	for i := 0; i < len(hostInfos); i++ {
		hostInfos[i] = c.HostConstructor(&HostContext{i, c.Start, hostMetricCount[i], epochsToLive[i]})
	}

	// This is not an optimal upper limit as it doesn't take into account host liveness but should be good enough
	maxPoints := epochs * c.HostCount
	if limit > 0 && limit < maxPoints {
		maxPoints = limit
	}
	dg := &GenericMetricsSimulator{
		commonDevopsSimulator: &commonDevopsSimulator{
			madePoints: 0,
			maxPoints:  maxPoints,

			hostIndex: 0,
			hosts:     hostInfos,

			epoch:          0,
			epochs:         epochs,
			epochHosts:     c.InitHostCount,
			initHosts:      c.InitHostCount,
			timestampStart: c.Start,
			timestampEnd:   c.End,
			interval:       interval,
		},
	}

	return dg
}

// Fields returns a map of subsystems to metrics collected
// Since each host has different number of fields (we use zipf distribution to assign # fields) we search
// for the host with the max number of fields
func (gms *GenericMetricsSimulator) Fields() map[string][]string {
	maxIndex := 0
	for i, h := range gms.hosts {
		if h.GenericMetricCount > gms.hosts[maxIndex].GenericMetricCount {
			maxIndex = i
		}
	}
	return gms.fields(gms.hosts[maxIndex].SimulatedMeasurements[:1])
}

func (gms *GenericMetricsSimulator) Headers() *common.GeneratedDataHeaders {
	return &common.GeneratedDataHeaders{
		TagKeys:   gms.TagKeys(),
		TagTypes:  gms.TagTypes(),
		FieldKeys: gms.Fields(),
	}
}

// Next advances a Point to the next state in the generator.
func (gms *GenericMetricsSimulator) Next(p *data.Point) bool {
	if gms.hostIndex >= uint64(len(gms.hosts)) {
		// we ended here b/c we reach the host limit
		// let's restart from the 1st host
		gms.hostIndex = 0
		// advance time & measurements for all the hosts. Note that this will advance
		// measurements for non started hosts as well - not an optimal but should be good enought
		for _, h := range gms.hosts {
			h.TickAll(gms.interval)
		}
		// increment epoch and adjust epoch hosts
		gms.adjustNumHostsForEpoch()
	}

	if gms.hostIndex < gms.epochHosts {
		host := &gms.hosts[gms.hostIndex]
		if host.StartEpoch == math.MaxUint64 {
			// mark the start time of the host
			host.StartEpoch = gms.epoch
		}
		if host.EpochsToLive == 0 {
			// found forever living host
			return gms.populatePoint(p, 0)
		}
		// check short-lived host life span
		if host.StartEpoch+host.EpochsToLive > gms.epoch {
			return gms.populatePoint(p, 0)
		}
	}

	// otherwise just move to the next host
	gms.hostIndex++
	gms.madePoints++
	return false
}
