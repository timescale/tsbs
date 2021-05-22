package serialize

import (
	"bytes"
	"fmt"
	"github.com/timescale/tsbs/pkg/data"
	"testing"
	"time"
)

var (
	TestNow         = time.Unix(1451606400, 0)
	TestMeasurement = []byte("cpu")
	TestTagKeys     = [][]byte{[]byte("hostname"), []byte("region"), []byte("datacenter")}
	TestTagVals     = []interface{}{"host_0", "eu-west-1", "eu-west-1b"}
	TestColFloat    = []byte("usage_guest_nice")
	TestColInt      = []byte("usage_guest")
	TestColInt64    = []byte("big_usage_guest")
)

const (
	TestFloat             = float64(38.24311829)
	TestInt               = 38
	TestInt64             = int64(5000000000)
	ErrWriterAlwaysErr    = "bad write: I always error"
	ErrWriterSometimesErr = "bad write: I sometimes error"
)

type ErrWriter struct {
	SkipOne bool
	Cnt     int
}

func (w *ErrWriter) Write(p []byte) (n int, err error) {
	if !w.SkipOne {
		return 0, fmt.Errorf(ErrWriterAlwaysErr)
	} else if w.Cnt < 1 {
		w.Cnt++
		return len(p), nil
	} else {
		return 0, fmt.Errorf(ErrWriterSometimesErr)
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

func TestPointDefault() *data.Point {
	return generateTestPoint(TestMeasurement, TestTagKeys, TestTagVals, &TestNow,
		[][]byte{TestColFloat}, []interface{}{TestFloat})
}

func TestPointMultiField() *data.Point {
	return generateTestPoint(TestMeasurement, TestTagKeys, TestTagVals,
		&TestNow, [][]byte{TestColInt64, TestColInt, TestColFloat},
		[]interface{}{TestInt64, TestInt, TestFloat})
}

func TestPointInt() *data.Point {
	return generateTestPoint(TestMeasurement, TestTagKeys, TestTagVals, &TestNow,
		[][]byte{TestColInt}, []interface{}{TestInt})
}

func TestPointNoTags() *data.Point {
	return generateTestPoint(TestMeasurement, [][]byte{}, []interface{}{}, &TestNow,
		[][]byte{TestColFloat}, []interface{}{TestFloat})
}

func TestPointWithNilTag() *data.Point {
	return generateTestPoint(TestMeasurement, [][]byte{[]byte("hostname")}, []interface{}{nil}, &TestNow,
		[][]byte{TestColFloat}, []interface{}{TestFloat})
}

func TestPointWithNilField() *data.Point {
	return generateTestPoint(TestMeasurement, [][]byte{}, []interface{}{}, &TestNow,
		[][]byte{TestColInt64, TestColFloat}, []interface{}{nil, TestFloat})
}

type SerializeCase struct {
	Desc       string
	InputPoint *data.Point
	Output     string
}

func SerializerTest(t *testing.T, cases []SerializeCase, ps PointSerializer) {
	for _, c := range cases {
		b := new(bytes.Buffer)
		ps.Serialize(c.InputPoint, b)
		got := b.String()
		if got != c.Output {
			t.Errorf("%s \nOutput incorrect: \nWant: '%s' \nGot:  '%s'", c.Desc, c.Output, got)
		}
	}
}
