package serialize

import (
	"bytes"
	"testing"
	"time"
)

var (
	testNow         = time.Unix(1451606400, 0)
	testMeasurement = []byte("cpu")
	testTagKeys     = [][]byte{[]byte("hostname"), []byte("region"), []byte("datacenter")}
	testTagVals     = [][]byte{[]byte("host_0"), []byte("eu-west-1"), []byte("eu-west-1b")}
	testColFloat    = []byte("usage_guest_nice")
	testColInt      = []byte("usage_guest")
	testColInt64    = []byte("big_usage_guest")
)

const (
	testFloat = float64(38.24311829)
	testInt   = 38
	testInt64 = int64(5000000000)
)

var testPointDefault = &Point{
	measurementName: testMeasurement,
	tagKeys:         testTagKeys,
	tagValues:       testTagVals,
	timestamp:       &testNow,
	fieldKeys:       [][]byte{testColFloat},
	fieldValues:     []interface{}{testFloat},
}

var testPointMultiField = &Point{
	measurementName: testMeasurement,
	tagKeys:         testTagKeys,
	tagValues:       testTagVals,
	timestamp:       &testNow,
	fieldKeys:       [][]byte{testColInt64, testColInt, testColFloat},
	fieldValues:     []interface{}{testInt64, testInt, testFloat},
}

var testPointInt = &Point{
	measurementName: testMeasurement,
	tagKeys:         testTagKeys,
	tagValues:       testTagVals,
	timestamp:       &testNow,
	fieldKeys:       [][]byte{testColInt},
	fieldValues:     []interface{}{testInt},
}

var testPointNoTags = &Point{
	measurementName: testMeasurement,
	tagKeys:         [][]byte{},
	tagValues:       [][]byte{},
	timestamp:       &testNow,
	fieldKeys:       [][]byte{testColFloat},
	fieldValues:     []interface{}{testFloat},
}

type serializeCase struct {
	desc       string
	inputPoint *Point
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
