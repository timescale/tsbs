package serialize

import (
	"testing"
)

func TestTimescaleDBSerializerSerialize(t *testing.T) {
	cases := []serializeCase{
		{
			desc:       "a regular Point",
			inputPoint: testPointDefault,
			output:     "tags,host_0,eu-west-1,eu-west-1b\ncpu,1451606400000000000,38.24311829\n",
		},
		{
			desc:       "a regular Point using int as value",
			inputPoint: testPointInt,
			output:     "tags,host_0,eu-west-1,eu-west-1b\ncpu,1451606400000000000,38\n",
		},
		{
			desc:       "a regular Point with multiple fields",
			inputPoint: testPointMultiField,
			output:     "tags,host_0,eu-west-1,eu-west-1b\ncpu,1451606400000000000,5000000000,38,38.24311829\n",
		},
		{
			desc:       "a Point with no tags",
			inputPoint: testPointNoTags,
			output:     "tags\ncpu,1451606400000000000,38.24311829\n",
		},
	}

	testSerializer(t, cases, &TimescaleDBSerializer{})
}
