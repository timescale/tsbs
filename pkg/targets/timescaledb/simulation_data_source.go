package timescaledb

import (
	"fmt"
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/serialize"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"github.com/timescale/tsbs/pkg/targets"
	"sync"
)

func newSimulationDataSource(sim common.Simulator) targets.DataSource {
	return &simulationDataSource{
		simulator: sim,
		headers:   sim.Headers(),
		pointPool: &sync.Pool{New: func() interface{} {
			return data.NewPoint()
		}},
	}
}

type simulationDataSource struct {
	simulator common.Simulator
	headers   *common.GeneratedDataHeaders
	pointPool *sync.Pool
}

func (d *simulationDataSource) Headers() *common.GeneratedDataHeaders {
	if d.headers != nil {
		return d.headers
	}

	d.headers = d.simulator.Headers()
	return d.headers
}

func (d *simulationDataSource) NextItem() *data.LoadedPoint {
	if d.headers == nil {
		fatal("headers not read before starting to read points")
		return nil
	}
	newSimulatorPoint := d.pointPool.Get().(*data.Point)
	defer d.pointPool.Put(newSimulatorPoint)
	defer newSimulatorPoint.Reset()
	d.simulator.Next(newSimulatorPoint)
	newLoadPoint := &insertData{}

	tagValues := newSimulatorPoint.TagValues()
	tagKeys := newSimulatorPoint.TagKeys()
	buf := make([]byte, 0, 256)
	for i, v := range tagValues {
		buf = append(buf, ',')
		buf = append(buf, tagKeys[i]...)
		buf = append(buf, '=')
		buf = serialize.FastFormatAppend(v, buf)
	}
	newLoadPoint.tags = string(buf)
	buf = buf[:0]
	buf = append(buf, []byte(fmt.Sprintf("%d", newSimulatorPoint.Timestamp().UTC().UnixNano()))...)
	fieldValues := newSimulatorPoint.FieldValues()
	for _, v := range fieldValues {
		buf = append(buf, ',')
		buf = serialize.FastFormatAppend(v, buf)
	}

	newLoadPoint.fields = string(buf)

	return data.NewLoadedPoint(&point{
		hypertable: string(newSimulatorPoint.MeasurementName()),
		row:        newLoadPoint,
	})
}
