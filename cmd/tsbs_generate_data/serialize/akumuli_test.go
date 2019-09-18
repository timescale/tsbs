package serialize

import (
	"bytes"
	"strings"
	"testing"
)

func TestAkumuliSerializerSerialize(t *testing.T) {

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
