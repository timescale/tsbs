package serialize

import (
	"testing"
)

func TestTimescaleDBSerializerSerialize(t *testing.T) {
	cases := []serializeCase{
		{
			desc:       "a regular Point",
			inputPoint: testPointDefault,
			output:     "tags,hostname=host_0,region=eu-west-1,datacenter=eu-west-1b\ncpu,1451606400000000000,38.24311829\n",
		},
		{
			desc:       "a regular Point using int as value",
			inputPoint: testPointInt,
			output:     "tags,hostname=host_0,region=eu-west-1,datacenter=eu-west-1b\ncpu,1451606400000000000,38\n",
		},
		{
			desc:       "a regular Point with multiple fields",
			inputPoint: testPointMultiField,
			output:     "tags,hostname=host_0,region=eu-west-1,datacenter=eu-west-1b\ncpu,1451606400000000000,5000000000,38,38.24311829\n",
		},
		{
			desc:       "a Point with no tags",
			inputPoint: testPointNoTags,
			output:     "tags\ncpu,1451606400000000000,38.24311829\n",
		},
	}

	testSerializer(t, cases, &TimescaleDBSerializer{})
}

func TestTimescaleDBSerializerSerializeErr(t *testing.T) {
	p := testPointMultiField
	s := &TimescaleDBSerializer{}
	err := s.Serialize(p, &errWriter{})
	if err == nil {
		t.Errorf("no error returned when expected")
	} else if err.Error() != errWriterAlwaysErr {
		t.Errorf("unexpected writer error: %v", err)
	}
}
