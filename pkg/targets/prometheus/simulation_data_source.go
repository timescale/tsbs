package prometheus

import (
	"log"
	"time"

	"github.com/timescale/promscale/pkg/prompb"
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

func (d *simulationDataSource) NextItem() data.LoadedPoint {
	if d.generatedSeries.HasNext() {
		next := d.generatedSeries.Next()
		return data.LoadedPoint{Data: next}
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
		return data.LoadedPoint{}
	}

	err := d.generatedSeries.Set(newSimulatorPoint)
	if err != nil {
		log.Printf("Couldn't convert simulated point to Prometheus TimeSeries: %v", err)
	}
	next := d.generatedSeries.Next()
	return data.LoadedPoint{Data: next}
}

type timeSeriesIterator struct {
	useCurrentTime  bool
	generatedSeries []prompb.TimeSeries
	currentInd      int
	lastTsUsed      int64
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

// Set converts the simulated point to []prompb.TimeSeries.
// Each point can contain N different metrics with the same
// label set and a single value for each metric. This is converted
// to N time series with a single sample.
func (t *timeSeriesIterator) Set(p *data.Point) error {
	// reset state of iterator
	t.currentInd = 0
	t.generatedSeries = make([]prompb.TimeSeries, len(p.FieldKeys()))
	err := convertToPromSeries(p, t.generatedSeries)
	if err != nil {
		return err
	}
	if t.useCurrentTime {
		t.updateTimestamps()
	}

	return nil
}

// updateTimestamps makes sure that two subsequent simulated points don't have
// the same timestamp (as represented in unix ms)
func (t *timeSeriesIterator) updateTimestamps() {
	currentTimeMs := time.Now().UnixNano() / 1000000
	if currentTimeMs > t.lastTsUsed {
		t.lastTsUsed = currentTimeMs
	} else {
		t.lastTsUsed++
	}
	for _, sample := range t.generatedSeries {
		// prometheus always generates single sample series
		// for remote_write endpoints
		sample.Samples[0].Timestamp = t.lastTsUsed
	}
}
