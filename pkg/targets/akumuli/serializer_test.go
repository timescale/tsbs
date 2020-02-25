package akumuli

import (
	"bytes"
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/serialize"
	"strings"
	"testing"
)

func TestAkumuliSerializerSerialize(t *testing.T) {

	serializer := NewAkumuliSerializer()

	points := []*data.Point{
		serialize.TestPointDefault(),
		serialize.TestPointInt(),
		serialize.TestPointMultiField(),
		serialize.TestPointDefault(),
		serialize.TestPointInt(),
		serialize.TestPointMultiField(),
	}

	type testCase struct {
		expCount int
		expValue string
		name     string
	}

	cases := []testCase{
		{
			expCount: 1,
			expValue: "+cpu.usage_guest_nice  hostname=host_0 region=eu-west-1 datacenter=eu-west-1b",
			name:     "series name default",
		},
		{
			expCount: 1,
			expValue: "+cpu.usage_guest  hostname=host_0 region=eu-west-1 datacenter=eu-west-1b",
			name:     "series name int",
		},
		{
			expCount: 1,
			expValue: "+cpu.big_usage_guest|cpu.usage_guest|cpu.usage_guest_nice  hostname=host_0 region=eu-west-1 datacenter=eu-west-1b",
			name:     "series name multi-field",
		},
		{
			expCount: 2,
			expValue: "*1\n+38.24311829",
			name:     "value default",
		},
		{
			expCount: 2,
			expValue: "*1\n:38",
			name:     "value int",
		},
		{
			expCount: 2,
			expValue: "*3\n:5000000000\n:38\n+38.24311829",
			name:     "value multi-field",
		},
		{
			expCount: 6,
			expValue: ":1451606400000000000",
			name:     "timestamp",
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
