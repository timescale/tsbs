package crate

import (
	"github.com/timescale/tsbs/pkg/data/serialize"
	"testing"
)

func TestCrateDBSerializerSerialize(t *testing.T) {
	cases := []serialize.SerializeCase{
		{
			Desc:       "a regular Point",
			InputPoint: serialize.TestPointDefault(),
			Output:     "cpu\t{\"hostname\":\"host_0\",\"region\":\"eu-west-1\",\"datacenter\":\"eu-west-1b\"}\t1451606400000000000\t38.24311829\n",
		},
		{
			Desc:       "a regular Point using int as value",
			InputPoint: serialize.TestPointInt(),
			Output:     "cpu\t{\"hostname\":\"host_0\",\"region\":\"eu-west-1\",\"datacenter\":\"eu-west-1b\"}\t1451606400000000000\t38\n",
		},
		{
			Desc:       "a regular Point with multiple fields",
			InputPoint: serialize.TestPointMultiField(),
			Output:     "cpu\t{\"hostname\":\"host_0\",\"region\":\"eu-west-1\",\"datacenter\":\"eu-west-1b\"}\t1451606400000000000\t5000000000\t38\t38.24311829\n",
		},
		{
			Desc:       "a Point with no tags",
			InputPoint: serialize.TestPointNoTags(),
			Output:     "cpu\tnull\t1451606400000000000\t38.24311829\n",
		},
	}

	serialize.SerializerTest(t, cases, &Serializer{})
}

func TestCrateDBSerializerSerializeErr(t *testing.T) {
	p := serialize.TestPointMultiField()
	s := &Serializer{}
	err := s.Serialize(p, &serialize.ErrWriter{})
	if err == nil {
		t.Errorf("no error returned when expected")
	} else if err.Error() != serialize.ErrWriterAlwaysErr {
		t.Errorf("unexpected writer error: %v", err)
	}
}
