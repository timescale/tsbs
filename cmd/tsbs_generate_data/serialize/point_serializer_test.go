package serialize

import (
	"bytes"
	"fmt"
	"github.com/timescale/tsbs/pkg/data"
	"testing"
	"time"
)

var (
	testNow         = time.Unix(1451606400, 0)
	testMeasurement = []byte("cpu")
	testTagKeys     = [][]byte{[]byte("hostname"), []byte("region"), []byte("datacenter")}
	testTagVals     = []interface{}{"host_0", "eu-west-1", "eu-west-1b"}
	testColFloat    = []byte("usage_guest_nice")
	testColInt      = []byte("usage_guest")
	testColInt64    = []byte("big_usage_guest")
)

const (
	testFloat             = float64(38.24311829)
	testInt               = 38
	testInt64             = int64(5000000000)
	errWriterAlwaysErr    = "bad write: I always error"
	errWriterSometimesErr = "bad write: I sometimes error"
)

type errWriter struct {
	skipOne bool
	cnt     int
}

func (w *errWriter) Write(p []byte) (n int, err error) {
	if !w.skipOne {
		return 0, fmt.Errorf(errWriterAlwaysErr)
	} else if w.cnt < 1 {
		w.cnt++
		return len(p), nil
	} else {
		return 0, fmt.Errorf(errWriterSometimesErr)
	}
}

func generateTestPoint(name []byte, tagKeys [][]byte, tagVals []interface{}, ts *time.Time, fieldKeys [][]byte, fieldValues []interface{}) *data.Point {
	p := &data.Point{}
	p.SetMeasurementName(name)
	p.SetTimestamp(ts)
	for i, tagKey := range tagKeys {
		p.AppendTag(tagKey, tagVals[i])
	}
	for i, fieldKey := range fieldKeys {
		p.AppendField(fieldKey, fieldValues[i])
	}
	return p
}

var testPointDefault = generateTestPoint(testMeasurement, testTagKeys, testTagVals, &testNow,
	[][]byte{testColFloat}, []interface{}{testFloat})

var testPointMultiField = generateTestPoint(testMeasurement, testTagKeys, testTagVals,
	&testNow, [][]byte{testColInt64, testColInt, testColFloat},
	[]interface{}{testInt64, testInt, testFloat})

var testPointInt = generateTestPoint(testMeasurement, testTagKeys, testTagVals, &testNow,
	[][]byte{testColInt}, []interface{}{testInt})

var testPointNoTags = generateTestPoint(testMeasurement, [][]byte{}, []interface{}{}, &testNow,
	[][]byte{testColFloat}, []interface{}{testFloat})

var testPointWithNilTag = generateTestPoint(testMeasurement, [][]byte{[]byte("hostname")}, []interface{}{nil}, &testNow,
	[][]byte{testColFloat}, []interface{}{testFloat})

var testPointWithNilField = generateTestPoint(testMeasurement, [][]byte{}, []interface{}{}, &testNow,
	[][]byte{testColInt64, testColFloat}, []interface{}{nil, testFloat})

type serializeCase struct {
	desc       string
	inputPoint *data.Point
	output     string
}

func testSerializer(t *testing.T, cases []serializeCase, ps PointSerializer) {
	for _, c := range cases {
		b := new(bytes.Buffer)
		ps.Serialize(c.inputPoint, b)
		got := b.String()
		if got != c.output {
			t.Errorf("%s \nOutput incorrect: \nWant: '%s' \nGot:  '%s'", c.desc, c.output, got)
		}
	}
}
