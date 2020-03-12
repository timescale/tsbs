package prometheus

import (
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"github.com/timescale/tsbs/pkg/targets"
)

func newSimulationDataSource(sim common.Simulator) targets.DataSource {
	return &simulationDataSource{
		simulator: sim,
		headers:   sim.Headers(),
	}
}

type simulationDataSource struct {
	simulator common.Simulator
	headers   *common.GeneratedDataHeaders
}

func (d *simulationDataSource) Headers() *common.GeneratedDataHeaders {
	if d.headers != nil {
		return d.headers
	}

	d.headers = d.simulator.Headers()
	return d.headers
}

func (d *simulationDataSource) NextItem() *data.LoadedPoint {
	newSimulatorPoint := data.NewPoint()
	var write bool
	for !d.simulator.Finished() {
		write = d.simulator.Next(newSimulatorPoint)
		if write {
			break
		}
		newSimulatorPoint.Reset()
	}
	if d.simulator.Finished() || !write {
		return nil
	}

	series := convertToPromSeries(newSimulatorPoint)
	return &data.LoadedPoint{Data: series}
}
