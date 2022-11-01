package iotdb

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/serialize"
)

func TestSerialize_001(t *testing.T) {
	cases := []struct {
		description string
		inputPoint  *data.Point
		expected    string
	}{
		{
			description: "a regular point ",
			inputPoint:  serialize.TestPointDefault(),
			expected:    "deviceID,timestamp,region,datacenter,usage_guest_nice\nroot.cpu.host_0,1451606400000000,'eu-west-1','eu-west-1b',38.24311829\n",
		},
		{
			description: "a regular Point using int as value",
			inputPoint:  serialize.TestPointInt(),
			expected:    "deviceID,timestamp,region,datacenter,usage_guest\nroot.cpu.host_0,1451606400000000,'eu-west-1','eu-west-1b',38\n",
		},
		{
			description: "a regular Point with multiple fields",
			inputPoint:  serialize.TestPointMultiField(),
			expected:    "deviceID,timestamp,region,datacenter,big_usage_guest,usage_guest,usage_guest_nice\nroot.cpu.host_0,1451606400000000,'eu-west-1','eu-west-1b',5000000000,38,38.24311829\n",
		},
		{
			description: "a Point with no tags",
			inputPoint:  serialize.TestPointNoTags(),
			expected:    "deviceID,timestamp,usage_guest_nice\nroot.cpu.unknown,1451606400000000,38.24311829\n",
		},
	}
	for _, c := range cases {
		t.Run(c.description, func(t *testing.T) {
			ps := &Serializer{}
			b := new(bytes.Buffer)
			err := ps.Serialize(c.inputPoint, b)
			require.NoError(t, err)
			actual := b.String()

			require.EqualValues(t, c.expected, actual)
		})
	}

}
