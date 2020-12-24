package timescaledb

import (
	"github.com/timescale/tsbs/pkg/data/serialize"
	"testing"
)

func TestTimescaleDBSerializerSerialize(t *testing.T) {
	cases := []serialize.SerializeCase{
		{
			Desc:       "a regular Point",
			InputPoint: serialize.TestPointDefault(),
			Output:     "tags,hostname=host_0,region=eu-west-1,datacenter=eu-west-1b\ncpu,1451606400000000000,38.24311829\n",
		},
		{
			Desc:       "a regular Point using int as value",
			InputPoint: serialize.TestPointInt(),
			Output:     "tags,hostname=host_0,region=eu-west-1,datacenter=eu-west-1b\ncpu,1451606400000000000,38\n",
		},
		{
			Desc:       "a regular Point with multiple fields",
			InputPoint: serialize.TestPointMultiField(),
			Output:     "tags,hostname=host_0,region=eu-west-1,datacenter=eu-west-1b\ncpu,1451606400000000000,5000000000,38,38.24311829\n",
		},
		{
			Desc:       "a Point with no tags",
			InputPoint: serialize.TestPointNoTags(),
			Output:     "tags\ncpu,1451606400000000000,38.24311829\n",
		},
	}

	serialize.SerializerTest(t, cases, &Serializer{})
}

func TestTimescaleDBSerializerSerializeErr(t *testing.T) {
	p := serialize.TestPointMultiField()
	s := &Serializer{}
	err := s.Serialize(p, &serialize.ErrWriter{})
	if err == nil {
		t.Errorf("no error returned when expected")
	} else if err.Error() != serialize.ErrWriterAlwaysErr {
		t.Errorf("unexpected writer error: %v", err)
	}
}
