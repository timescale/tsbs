package ceresdb

import (
	"fmt"
	"time"

	"github.com/CeresDB/ceresdb-client-go/ceresdb"
	"github.com/influxdata/line-protocol/v2/lineprotocol"
	"github.com/timescale/tsbs/pkg/data"
)

type batch struct {
	pointCount uint64
	fieldCount uint64
	points     []ceresdb.Point
}

func (b *batch) Len() uint {
	return uint(b.pointCount)
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
	dec := lineprotocol.NewDecoderWithBytes(item.Data.([]byte))

	for dec.Next() {
		m, err := dec.Measurement()
		if err != nil {
			panic(err)
		}

		builder := ceresdb.NewPointBuilder(string(m))

		for {
			key, val, err := dec.NextTag()
			if err != nil {
				panic(err)
			}
			if key == nil {
				break
			}
			builder.AddTag(string(key), ceresdb.NewStringValue(string(val)))
		}

		for {
			key, val, err := dec.NextField()
			if err != nil {
				panic(err)
			}
			if key == nil {
				break
			}
			builder.AddField(string(key), ceresdb.NewDoubleValue(valueToFloat64(val)))
		}

		timestamp, err := dec.Time(lineprotocol.Nanosecond, time.Time{})
		if err != nil {
			panic(err)
		}
		builder.SetTimestamp(unixTimestampMs(timestamp))

		point, err := builder.Build()
		if err != nil {
			panic(err)
		}

		b.points = append(b.points, point)
		b.pointCount++
		b.fieldCount += uint64(len(point.Fields))
	}
}
