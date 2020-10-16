package prometheus

import (
	"bufio"
	"bytes"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/timescale/promscale/pkg/prompb"
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/serialize"
)

func TestPrometheusSerializer(t *testing.T) {
	var buffer bytes.Buffer
	ser := Serializer{}
	err := ser.Serialize(serialize.TestPointDefault(), &buffer)
	if err != nil {
		t.Errorf("error while serializing point: %v", err)
	}
	promIter, err := NewPrometheusIterator(bufio.NewReader(&buffer))
	if err != nil {
		t.Errorf("error while creating iterator: %v", err)
	}

	series := make([]*prompb.TimeSeries, 0)

	for promIter.HasNext() {
		ts, err := promIter.Next()
		if err != nil {
			t.Errorf("error getting next: %v", err)
		}
		series = append(series, ts)
	}

	assertEqual(1, len(series), t, "Wrong number of series")

	ts := series[0]
	assertEqual(serialize.TestFloat, ts.Samples[0].GetValue(), t, "bad sample value")
	assertEqual(serialize.TestPointDefault().Timestamp().UnixNano()/1000000, ts.Samples[0].GetTimestamp(), t, "wrong timestamp")

	// We add metric name as label into TimeSeries
	assertEqual(len(serialize.TestPointDefault().TagKeys())+1, len(ts.Labels), t, "wrong number of tags")

	// Lets test point with multiple fields. We generate new TimeSeries for each field
	buffer.Reset()
	series = nil
	ser = Serializer{}
	err = ser.Serialize(serialize.TestPointMultiField(), &buffer)
	if err != nil {
		t.Errorf("error while serializing: %v", err)
	}

	promIter, err = NewPrometheusIterator(bufio.NewReader(&buffer))
	if err != nil {
		t.Errorf("error creating deserializer: %v", err)
	}

	for promIter.HasNext() {
		ts, err := promIter.Next()
		if err != nil {
			t.Errorf("error getting next protobuf: %v", err)
		}
		series = append(series, ts)
	}

	assertEqual(len(serialize.TestPointMultiField().FieldKeys()), len(series), t, "wrong number of series")
	assertEqual(len(serialize.TestPointMultiField().FieldKeys()), int(promIter.processed), t, "wrong iterator state")
	if series == nil {
		t.Errorf("expected series to != nil")
	} else {
		assertEqual(float64(serialize.TestInt64), series[0].Samples[0].GetValue(), t, "wrong sample value")
	}
}

func assertEqual(expected, got interface{}, t *testing.T, msg string) {
	if expected != got {
		t.Error(fmt.Sprintf("%s. Expected: %v, Got: %v", msg, expected, got))
	}
}

func TestConvertToPromSeries(t *testing.T) {
	someTimeAgo := time.Now().Add(-time.Second)
	oneFieldPoint := data.NewPoint()
	oneFieldPoint.SetTimestamp(&someTimeAgo)
	oneFieldPoint.AppendField([]byte("f"), 1)
	ofTS := prompb.TimeSeries{
		Labels:  []prompb.Label{{Name: "__name__", Value: "f"}},
		Samples: []prompb.Sample{{Value: 1, Timestamp: oneFieldPoint.Timestamp().UnixNano() / 1000000}},
	}

	twoFieldPoint := data.NewPoint()
	twoFieldPoint.SetTimestamp(&someTimeAgo)
	twoFieldPoint.AppendField([]byte("f"), 1)
	twoFieldPoint.AppendField([]byte("g"), 2)
	twoFieldPoint.AppendTag([]byte("b"), "t1")
	twoFieldPoint.AppendTag([]byte("a"), "t2")
	tfTS1 := prompb.TimeSeries{
		Labels:  []prompb.Label{{Name: "__name__", Value: "f"}, {Name: "a", Value: "t2"}, {Name: "b", Value: "t1"}},
		Samples: []prompb.Sample{{Value: 1, Timestamp: twoFieldPoint.Timestamp().UnixNano() / 1000000}},
	}
	tfTS2 := prompb.TimeSeries{
		Labels:  []prompb.Label{{Name: "__name__", Value: "g"}, {Name: "a", Value: "t2"}, {Name: "b", Value: "t1"}},
		Samples: []prompb.Sample{{Value: 2, Timestamp: twoFieldPoint.Timestamp().UnixNano() / 1000000}},
	}

	testCases := []struct {
		desc      string
		expError  bool
		inPoint   *data.Point
		inBuffer  []prompb.TimeSeries
		expBuffer []prompb.TimeSeries
	}{
		{desc: "Error on wrong size buffer", expError: true, inPoint: oneFieldPoint, inBuffer: []prompb.TimeSeries{}},
		{
			desc:      "Single field, single time-series",
			inPoint:   oneFieldPoint,
			inBuffer:  make([]prompb.TimeSeries, 1),
			expBuffer: []prompb.TimeSeries{ofTS},
		}, {
			desc:      "Two fields, two time-series, labels sorted",
			inPoint:   twoFieldPoint,
			inBuffer:  make([]prompb.TimeSeries, 2),
			expBuffer: []prompb.TimeSeries{tfTS1, tfTS2},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			err := convertToPromSeries(tc.inPoint, tc.inBuffer)
			if tc.expError && err != nil {
				return
			} else if tc.expError {
				t.Errorf("unexpected lack of error")
				return
			} else if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			for i, ts := range tc.expBuffer {
				returnedTS := tc.inBuffer[i]
				if len(returnedTS.Samples) != len(ts.Samples) {
					t.Errorf("wrong samples size; exp: %d; got %d", len(returnedTS.Samples), len(ts.Samples))
					return
				}
				labelsAreSorted := sort.SliceIsSorted(returnedTS.Labels, func(i, j int) bool {
					return returnedTS.Labels[i].Name < returnedTS.Labels[j].Name
				})
				if !labelsAreSorted {
					t.Errorf("labels not sorted by name: %v", returnedTS.Labels)
				}
				for j, retLabel := range returnedTS.Labels {
					expLabel := tc.expBuffer[i].Labels[j]
					if expLabel.Name != retLabel.Name {
						t.Errorf("labels nor ordered; exp: %s; got %s", expLabel.Name, retLabel.Name)
					}
					if expLabel.Value != retLabel.Value {
						t.Errorf("label value missmatch; exp: %s; got %s", expLabel.Value, retLabel.Value)
					}
				}
				for j, retSample := range returnedTS.Samples {
					expSample := tc.expBuffer[i].Samples[j]
					if retSample.Value != expSample.Value {
						t.Errorf("sample value missmatch; exp: %f ; got: %f", expSample.Value, retSample.Value)
					}
					if retSample.Timestamp != expSample.Timestamp {
						t.Errorf("sample time missmatch; exp: %d; got: %d", expSample.Timestamp, retSample.Timestamp)
					}
				}
			}
		})
	}
}
