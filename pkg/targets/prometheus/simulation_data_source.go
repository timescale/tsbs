package prometheus

import (
	"github.com/prometheus/prometheus/prompb"
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"github.com/timescale/tsbs/pkg/targets"
)

func newSimulationDataSource(sim common.Simulator, useCurrentTime bool) targets.DataSource {
	return &simulationDataSource{
		simulator:       sim,
		headers:         sim.Headers(),
		generatedSeries: &timeSeriesIterator{useCurrentTime: useCurrentTime},
	}
}

type simulationDataSource struct {
	simulator       common.Simulator
	headers         *common.GeneratedDataHeaders
	generatedSeries *timeSeriesIterator
}

func (d *simulationDataSource) Headers() *common.GeneratedDataHeaders {
	if d.headers != nil {
		return d.headers
	}

	d.headers = d.simulator.Headers()
	return d.headers
}

func (d *simulationDataSource) NextItem() *data.LoadedPoint {
	if d.generatedSeries.HasNext() {
		next := d.generatedSeries.Next()
		return &data.LoadedPoint{Data: next}
	}

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

	d.generatedSeries.Set(newSimulatorPoint)
	next := d.generatedSeries.Next()
	return &data.LoadedPoint{Data: next}
}

type timeSeriesIterator struct {
	useCurrentTime  bool
	generatedSeries []prompb.TimeSeries
	currentInd      int
}

func (t *timeSeriesIterator) HasNext() bool {
	return t.currentInd < len(t.generatedSeries)
}

func (t *timeSeriesIterator) Next() *prompb.TimeSeries {
	if !t.HasNext() {
		return nil
	}
	tmp := t.currentInd
	t.currentInd++
	return &t.generatedSeries[tmp]
}

func (t *timeSeriesIterator) Set(p *data.Point) {
	// reset state of iterator
	t.currentInd = 0
	t.resetBuffer(len(p.FieldKeys()))
	convertToPromSeries(p, t.generatedSeries, t.useCurrentTime)
}

func (t *timeSeriesIterator) resetBuffer(requiredLength int) {
	if t.generatedSeries == nil {
		t.generatedSeries = make([]prompb.TimeSeries, requiredLength)
		return
	}
	if len(t.generatedSeries) < requiredLength {
		t.generatedSeries = make([]prompb.TimeSeries, requiredLength)
	}
}
