package devops

import (
	"time"

	"bitbucket.org/440-labs/tsbs/cmd/tsbs_generate_data/common"
	"bitbucket.org/440-labs/tsbs/cmd/tsbs_generate_data/serialize"
)

// A CPUOnlySimulator generates data similar to telemetry from Telegraf for only CPU metrics.
// It fulfills the Simulator interface.
type CPUOnlySimulator struct {
	*commonDevopsSimulator
}

// Fields returns a map of subsystems to metrics collected
func (d *CPUOnlySimulator) Fields() map[string][][]byte {
	return d.fields(d.hosts[0].SimulatedMeasurements[:1])
}

// Next advances a Point to the next state in the generator.
func (d *CPUOnlySimulator) Next(p *serialize.Point) bool {
	// switch to the next metric if needed
	if d.hostIndex == uint64(len(d.hosts)) {
		d.hostIndex = 0

		for i := 0; i < len(d.hosts); i++ {
			d.hosts[i].TickAll(d.interval)
		}

		d.adjustNumHostsForEpoch()
	}

	return d.populatePoint(p, 0)
}

// CPUOnlySimulatorConfig is used to create a CPUOnlySimulator.
type CPUOnlySimulatorConfig struct {
	Start time.Time
	End   time.Time

	// InitHostCount is the number of hosts to start with in the first reporting period
	InitHostCount uint64
	// HostCount is the total number of hosts to have in the last reporting period
	HostCount       uint64
	HostConstructor func(i int, start time.Time) Host
}

// ToSimulator produces a Simulator that conforms to the given SimulatorConfig over the specified interval
func (d *CPUOnlySimulatorConfig) ToSimulator(interval time.Duration) common.Simulator {
	hostInfos := make([]Host, d.HostCount)
	for i := 0; i < len(hostInfos); i++ {
		hostInfos[i] = d.HostConstructor(i, d.Start)
	}

	epochs := uint64(d.End.Sub(d.Start).Nanoseconds() / interval.Nanoseconds())
	maxPoints := epochs * d.HostCount
	dg := &CPUOnlySimulator{&commonDevopsSimulator{
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
	}}

	return dg
}
