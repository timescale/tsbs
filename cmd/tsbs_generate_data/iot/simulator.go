package iot

import (
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_data/common"
)

// SimulatorConfig is used to create an IoT Simulator.
// It fulfills the common.SimulatorConfig interface.
type SimulatorConfig common.BaseSimulatorConfig

// NewSimulator produces an IoT Simulator with the given
// config over the specified interval and points limit.
func (sc *SimulatorConfig) NewSimulator(interval time.Duration, limit uint64) common.Simulator {
	return (*common.BaseSimulatorConfig)(sc).NewSimulator(interval, limit)
}
