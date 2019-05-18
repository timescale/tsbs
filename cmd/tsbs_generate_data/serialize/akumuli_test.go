package serialize

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestAkumuliSerializerSerialize(t *testing.T) {
	var (
		testNow         = time.Unix(1451606400, 0)
		testMeasurement = []byte("cpu")
		testTagKeys     = [][]byte{[]byte("hostname"), []byte("region"), []byte("datacenter")}
		testTagVals     = [][]byte{[]byte("host_0"), []byte("eu-west-1"), []byte("eu-west-1b")}
		testColFloat    = []byte("usage_guest_nice")
		testColInt      = []byte("usage_guest")
		testColInt64    = []byte("big_usage_guest")
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

	serializer := AkumuliSerializer{}

	points := []*Point{
		testPointDefault,
		testPointInt,
		testPointMultiField,
		testPointDefault,
		testPointInt,
		testPointMultiField,
	}

	type testCase struct {
		expCount int
		expValue string
		name     string
	}

	cases := []testCase{
		{
			1,
			"+cpu.usage_guest_nice  hostname=host_0 region=eu-west-1 datacenter=eu-west-1b",
			"series name default",
		},
		{
			1,
			"+cpu.usage_guest  hostname=host_0 region=eu-west-1 datacenter=eu-west-1b",
			"series name int",
		},
		{
			1,
			"+cpu.big_usage_guest|cpu.usage_guest|cpu.usage_guest_nice  hostname=host_0 region=eu-west-1 datacenter=eu-west-1b",
			"series name multi-field",
		},
		{
			2,
			"*1\n+38.24311829",
			"value default",
		},
		{
			2,
			"*1\n:38",
			"value int",
		},
		{
			2,
			"*3\n:5000000000\n:38\n+38.24311829",
			"value multi-field",
		},
		{
			6,
			":1451606400000000000",
			"timestamp",
		},
	}
	buf := new(bytes.Buffer)
	for _, point := range points {
		serializer.Serialize(point, buf)
	}

	got := buf.String()

	for _, c := range cases {
		actualCnt := strings.Count(got, c.expValue)
		if actualCnt != c.expCount {
			t.Errorf("Output incorrect: %s expected %d times got %d times", c.name, c.expCount, actualCnt)
		}
	}
}
