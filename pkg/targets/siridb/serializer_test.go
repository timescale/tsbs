package siridb

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/serialize"
	"io"
	"log"
	"testing"

	qpack "github.com/transceptor-technology/go-qpack"
)

func TestSiriDBSerializerSerialize(t *testing.T) {
	type output struct {
		seriename []string
		value     [][]interface{}
	}
	cases := []struct {
		desc       string
		inputPoint *data.Point
		want       output
	}{
		{
			desc:       "a regular Point",
			inputPoint: serialize.TestPointDefault(),
			want: output{
				seriename: []string{"cpu|hostname=host_0,region=eu-west-1,datacenter=eu-west-1b|usage_guest_nice"},
				value:     [][]interface{}{{1451606400000000000, 38.24311829}},
			},
		},
		{
			desc:       "a regular Point using int as value",
			inputPoint: serialize.TestPointInt(),
			want: output{
				seriename: []string{"cpu|hostname=host_0,region=eu-west-1,datacenter=eu-west-1b|usage_guest"},
				value:     [][]interface{}{{1451606400000000000, 38}},
			},
		},
		{
			desc:       "a regular Point with multiple fields",
			inputPoint: serialize.TestPointMultiField(),
			want: output{
				seriename: []string{
					"cpu|hostname=host_0,region=eu-west-1,datacenter=eu-west-1b|big_usage_guest",
					"cpu|hostname=host_0,region=eu-west-1,datacenter=eu-west-1b|usage_guest",
					"cpu|hostname=host_0,region=eu-west-1,datacenter=eu-west-1b|usage_guest_nice",
				},
				value: [][]interface{}{
					{1451606400000000000, 5000000000},
					{1451606400000000000, 38},
					{1451606400000000000, 38.24311829},
				},
			},
		},
		{
			desc:       "a Point with no tags",
			inputPoint: serialize.TestPointNoTags(),
			want: output{
				seriename: []string{"cpu||usage_guest_nice"},
				value:     [][]interface{}{{1451606400000000000, 38.24311829}},
			},
		},
	}

	ps := &Serializer{}
	d := &decoder{}
	for _, c := range cases {
		b := new(bytes.Buffer)
		ps.Serialize(c.inputPoint, b)
		br := bufio.NewReader(bytes.NewReader(b.Bytes()))
		key, data := d.deSerializeSiriDB(br)

		for i, k := range key {
			if got := k; got != c.want.seriename[i] {
				t.Errorf("%s \nOutput incorrect: \nWant: '%s' \nGot:  '%s'", c.desc, c.want.seriename[i], got)
			}

			var unpacked interface{}
			var err error
			if unpacked, err = qpack.Unpack(data[i], 1); err != nil {
				t.Errorf("%s", err)
			}

			switch v := unpacked.(type) {
			case []interface{}:
				for j, got := range v {
					if got != c.want.value[i][j] {
						t.Errorf("%s \nOutput incorrect: \nWant: '%s' \nGot:  '%s'", c.desc, c.want.value[i][j], got)
					}
				}
			default:
				t.Errorf("Qpack returned the incorrect type: %T", v)

			}

		}
	}
}

type decoder struct {
	buf []byte
	len uint32
}

func (d *decoder) Read(bf *bufio.Reader) int {
	buf := make([]byte, 8192)
	n, err := bf.Read(buf)
	if err == io.EOF {
		return n
	}
	if err != nil {
		log.Fatal(err.Error())
	}

	d.len += uint32(n)
	d.buf = append(d.buf, buf[:n]...)
	return n
}

func (d *decoder) deSerializeSiriDB(bf *bufio.Reader) ([]string, [][]byte) {
	if d.len < 8 {
		if n := d.Read(bf); n == 0 {
			return nil, nil
		}
	}
	valueCnt := binary.LittleEndian.Uint32(d.buf[:4])
	nameCnt := binary.LittleEndian.Uint32(d.buf[4:8])

	d.buf = d.buf[8:]
	d.len -= 8

	if d.len < nameCnt {
		if n := d.Read(bf); n == 0 {
			return nil, nil
		}
	}

	name := d.buf[:nameCnt]

	d.buf = d.buf[nameCnt:]
	d.len -= nameCnt

	key := make([]string, 0)
	data := make([][]byte, 0)
	for i := 0; uint32(i) < valueCnt; i++ {
		if d.len < 8 {
			if n := d.Read(bf); n == 0 {
				return nil, nil
			}
		}
		lengthKey := binary.LittleEndian.Uint32(d.buf[:4])
		lengthData := binary.LittleEndian.Uint32(d.buf[4:8])

		total := lengthData + lengthKey + 8
		for d.len < total {
			if n := d.Read(bf); n == 0 {
				return nil, nil
			}
		}

		key = append(key, string(name)+string(d.buf[8:lengthKey+8]))
		data = append(data, d.buf[lengthKey+8:total])

		d.buf = d.buf[total:]
		d.len -= total
	}
	return key, data
}

func TestSiriDBSerializerSerializeErr(t *testing.T) {
	p := serialize.TestPointMultiField()
	s := &Serializer{}
	err := s.Serialize(p, &serialize.ErrWriter{})
	if err == nil {
		t.Errorf("no error returned when expected")
	} else if err.Error() != serialize.ErrWriterAlwaysErr {
		t.Errorf("unexpected writer error: %v", err)
	}
}
