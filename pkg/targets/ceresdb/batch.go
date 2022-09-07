package ceresdb

import (
	"fmt"
	"time"

	"github.com/influxdata/line-protocol/v2/lineprotocol"
	"github.com/jiacai2050/ceresdb_client_go/ceresdb"
	"github.com/timescale/tsbs/pkg/data"
)

type batch struct {
	rows    uint64
	metrics uint64
	points  []ceresdb.Point
}

func (b *batch) Len() uint {
	return uint(b.rows)
}

func unixTimestampMs(t time.Time) int64 {
	return t.UnixNano() / int64(time.Millisecond)
}

func valueToFloat64(v lineprotocol.Value) float64 {
	switch v.Kind() {
	case lineprotocol.Int:
		return float64(v.IntV())
	case lineprotocol.Float:
		return v.FloatV()
	default:
		panic(fmt.Sprintf("not support field type: %v", v))
	}
}
func (b *batch) Append(item data.LoadedPoint) {
	data := item.Data.([]byte)
	dec := lineprotocol.NewDecoderWithBytes(data)

	for dec.Next() {
		b.rows++
		m, err := dec.Measurement()
		if err != nil {
			panic(err)
		}
		tags := make(map[string]string)
		for {
			key, val, err := dec.NextTag()
			if err != nil {
				panic(err)
			}
			if key == nil {
				break
			}
			tags[string(key)] = string(val)
		}
		fields := make(map[string]float64)
		for {
			key, val, err := dec.NextField()
			if err != nil {
				panic(err)
			}
			if key == nil {
				break
			}
			fields[string(key)] = valueToFloat64(val)
		}
		t, err := dec.Time(lineprotocol.Nanosecond, time.Time{})
		if err != nil {
			panic(err)
		}
		b.metrics += uint64(len(fields))

		b.points = append(b.points, ceresdb.Point{
			Metric:    string(m),
			Tags:      tags,
			Fields:    fields,
			Timestamp: unixTimestampMs(t),
		})
	}
}
