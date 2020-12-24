package prometheus

import (
	"github.com/timescale/promscale/pkg/prompb"
	"github.com/timescale/tsbs/pkg/data"
	"reflect"
	"testing"
	"time"
)

func TestTimeSeriesIterator(t *testing.T) {
	now := time.Now()
	emptyPoint := data.NewPoint()
	emptyPoint.SetTimestamp(&now)

	oneField := data.NewPoint()
	oneField.AppendTag([]byte("tag"), "tag")
	oneField.AppendField([]byte("m1"), float64(1))
	oneField.SetTimestamp(&now)

	twoFields := data.NewPoint()
	twoFields.AppendTag([]byte("tag"), "tag")
	twoFields.AppendField([]byte("m1"), float64(1))
	twoFields.AppendField([]byte("m2"), float64(2))
	twoFields.SetTimestamp(&now)

	cases := []struct {
		desc string
		set  *data.Point
	}{
		{
			desc: "No fields -> no time series",
			set:  emptyPoint,
		}, {
			desc: "One field one time series",
			set:  oneField,
		}, {
			desc: "Two fields two time series",
			set:  twoFields,
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			iter := timeSeriesIterator{}
			if iter.generatedSeries != nil || iter.currentInd != 0 {
				t.Errorf("unexpected initial iterator state; expected series: nil, currentInd: 0;"+
					" got: %v and %d", iter.generatedSeries, iter.currentInd)
				return
			}
			inputPoint := tc.set
			err := iter.Set(inputPoint)
			if err != nil {
				t.Error(err)
			}
			var generated []*prompb.TimeSeries
			for iter.HasNext() {
				generated = append(generated, iter.Next())
			}
			fieldKeys := inputPoint.FieldKeys()
			numExpectedTimeSeries := len(fieldKeys)
			if len(generated) != numExpectedTimeSeries {
				t.Errorf("expected time series for each field (total %d) got: %d",
					numExpectedTimeSeries, len(generated),
				)
			}

			for i, ts := range generated {
				if ts.Samples[0].Value != inputPoint.GetFieldValue(fieldKeys[i]) {
					t.Errorf("unexpected value in time-series; exp: %f; got: %f",
						inputPoint.GetFieldValue(fieldKeys[i]), ts.Samples[0].Value,
					)
				}
				for _, label := range ts.Labels {
					if label.Name == "__name__" {
						if label.Value != string(fieldKeys[i]) {
							t.Errorf("unexpected name for time-series: exp: %s; got %s",
								string(fieldKeys[i]), label.Value,
							)
						}
						continue
					}
					tagValue := inputPoint.GetTagValue([]byte(label.Name))
					if label.Value != tagValue.(string) {
						t.Errorf("unexpected tag in time-series; exp %s; got: %s",
							label.Value, tagValue,
						)
					}
				}
			}
			// check if properly reset after some generating has been done
			err = iter.Set(inputPoint)
			if err != nil {
				t.Error(err)
			}
			if len(iter.generatedSeries) != numExpectedTimeSeries {
				t.Errorf("after Set, iterator had unexpected generatedSeries len."+
					"Expected %d; got %d", len(iter.generatedSeries), numExpectedTimeSeries)
			}
			if iter.currentInd != 0 {
				t.Errorf("after Set, iterator currentInd not reset to 0; got: %d", iter.currentInd)
			}
		})
	}
}

func TestTimeSeriesIteratorMultipleSets(t *testing.T) {
	now := time.Now()
	emptyPoint := data.NewPoint()
	emptyPoint.SetTimestamp(&now)

	point := data.NewPoint()
	point.AppendTag([]byte("tag"), "tag")
	point.AppendField([]byte("m1"), float64(1))
	point.SetTimestamp(&now)
	promPoint := &prompb.TimeSeries{
		Labels:  []prompb.Label{{Name: "__name__", Value: "m1"}, {Name: "tag", Value: "tag"}},
		Samples: []prompb.Sample{{Timestamp: point.TimestampInUnixMs(), Value: float64(1)}},
	}

	futureTime := time.Now().AddDate(1, 0, 0).UnixNano() / 1000000
	promPointInFuture1 := &prompb.TimeSeries{
		Labels:  []prompb.Label{{Name: "__name__", Value: "m1"}, {Name: "tag", Value: "tag"}},
		Samples: []prompb.Sample{{Timestamp: futureTime + 1, Value: float64(1)}},
	}
	promPointInFuture2 := &prompb.TimeSeries{
		Labels:  []prompb.Label{{Name: "__name__", Value: "m1"}, {Name: "tag", Value: "tag"}},
		Samples: []prompb.Sample{{Timestamp: futureTime + 2, Value: float64(1)}},
	}
	cases := []struct {
		desc         string
		pointToSet   *data.Point
		useCurrentTs bool
		lastTsUsed   int64
		expect1      []*prompb.TimeSeries
		expect2      []*prompb.TimeSeries
	}{
		{
			desc:       "Use point time",
			pointToSet: point,
			expect1:    []*prompb.TimeSeries{promPoint},
			expect2:    []*prompb.TimeSeries{promPoint},
		}, {
			desc:         "Use current time, it is lagging behind last used timestamp",
			useCurrentTs: true,
			// use timestamp in future to simulate that now() is lagging
			lastTsUsed: futureTime,
			pointToSet: point,
			expect1:    []*prompb.TimeSeries{promPointInFuture1},
			expect2:    []*prompb.TimeSeries{promPointInFuture2},
		},
	}

	compareTSs := func(exp, got []*prompb.TimeSeries) bool {
		if len(exp) != len(got) {
			return false
		}
		for i, tsExp := range exp {
			tsGot := got[i]
			if !reflect.DeepEqual(tsExp.Labels, tsGot.Labels) {
				t.Errorf("labels missmatch; exp: %v; got %v", tsExp.Labels, tsGot.Labels)
				return false
			}
			if !reflect.DeepEqual(tsExp.Samples, tsGot.Samples) {
				t.Errorf("samples missmatch; exp: %v; got %v", tsExp.Samples, tsGot.Samples)
				return false
			}
		}
		return true
	}

	setPoint := func(p *data.Point, iter *timeSeriesIterator) []*prompb.TimeSeries {
		if err := iter.Set(p); err != nil {
			t.Error("could not set point:", err)
			return nil
		}
		var got []*prompb.TimeSeries
		for iter.HasNext() {
			got = append(got, iter.Next())
		}
		return got
	}
	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			iter := &timeSeriesIterator{
				useCurrentTime: tc.useCurrentTs,
				lastTsUsed:     tc.lastTsUsed,
			}
			got1 := setPoint(tc.pointToSet, iter)
			got2 := setPoint(tc.pointToSet, iter)
			compareTSs(tc.expect1, got1)
			compareTSs(tc.expect2, got2)
		})
	}
}
