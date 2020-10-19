package main

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"reflect"
	"testing"
	"time"

	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
)

func TestEventsBatch(t *testing.T) {
	f := &factory{}
	eb := f.New().(*eventsBatch)
	if eb.Len() != 0 {
		t.Errorf("eventBatch must be empty")
	}
	points := []data.LoadedPoint{
		{
			Data: &point{
				table: "type1",
				row: row{[]byte(
					"{\"t1\":\"v1\",\"t2\":\"v2\"}"),
					time.Unix(0, 1451606400000000000),
					55.0,
				},
			},
		},
		{
			Data: &point{
				table: "type2",
				row: row{
					[]byte("{\"t1\":\"v1\",\"t2\":\"v2\"}"),
					time.Unix(0, 1451606400000000000),
					55,
				},
			},
		},
		{
			Data: &point{
				table: "type2",
				row: row{
					[]byte("{\"t1\":\"v1\",\"t2\":\"v2\"}"),
					time.Unix(0, 1451606400000000000),
					0, 55.0, 1,
				},
			},
		},
		{
			Data: &point{
				table: "type2",
				row: row{
					[]byte("{\"t1\":\"v1\",\"t2\":\"v2\"}"),
					time.Unix(0, 1557147988108),
					11, 55.0,
				},
			},
		},
	}
	for _, p := range points {
		eb.Append(p)
	}
	if eb.Len() != uint(len(points)) {
		t.Errorf(fmt.Sprintf("eventsBatch must have %d points", len(points)))
	}
	if len(eb.batches) != 2 {
		t.Errorf("eventsBatch must have two measurement types")
	}
}

func TestDecode(t *testing.T) {
	cases := []struct {
		desc           string
		input          string
		expectedTable  string
		expectedRow    row
		expectedToFail bool
	}{
		{
			desc:          "correct input",
			input:         "cpu\t{\"hostname\":\"host_0\"}\t1454608400000000000\t38.243",
			expectedTable: "cpu",
			expectedRow: row{
				[]byte("{\"hostname\":\"host_0\"}"),
				time.Unix(0, 1454608400000000000),
				38.243, 1,
			},
		},
		{
			desc:          "correct input: empty tags",
			input:         "mem\tnull\t1454608400000000000\t38.24311829",
			expectedTable: "mem",
			expectedRow: row{
				[]byte("null"),
				time.Unix(0, 1454608400000000000),
				38.24311829,
			},
		},
		{
			desc:           "incorrect input:, missing timestamp",
			input:          "mem\tnull\t\t38.24311829",
			expectedToFail: true,
		},
		{
			desc:           "incorrect input: missing metrics",
			input:          "mem\tnull\t1454608400000000000",
			expectedToFail: true,
		},
		{
			desc:           "incorrect input: malformed",
			input:          "..",
			expectedToFail: true,
		},
	}
	for _, c := range cases {
		br := bufio.NewReader(bytes.NewReader([]byte(c.input)))
		decoder := &fileDataSource{scanner: bufio.NewScanner(br)}
		if c.expectedToFail {
			fmt.Println(c.desc)
			isCalled := false
			fatal = func(fmt string, args ...interface{}) {
				isCalled = true
				log.Printf(fmt, args...)
			}
			_ = decoder.NextItem()
			if !isCalled {
				t.Errorf("%s: did not call fatal when it should", c.desc)
			}
		} else {
			p := decoder.NextItem()
			point := p.Data.(*point)
			if point.table != c.expectedTable {
				t.Errorf(
					"%s: incorrect prefix: got %s want %s",
					c.desc,
					point.table,
					c.expectedTable,
				)
			}
			for i, value := range point.row {
				if !reflect.DeepEqual(value, c.expectedRow[i]) {
					t.Errorf(
						"%s: incorrect fields: got %s want %s",
						c.desc,
						value,
						c.expectedRow[i],
					)
				}
			}
		}
	}
}

func TestDecodeEOF(t *testing.T) {
	input := []byte("cpu\t{\"hostname\":\"host_0\"}\t1454608400000000000\t38.24311829\n")
	br := bufio.NewReader(bytes.NewReader([]byte(input)))
	decoder := &fileDataSource{scanner: bufio.NewScanner(br)}
	_ = decoder.NextItem()
	// nothing left, should be EOF
	p := decoder.NextItem()
	if p.Data != nil {
		t.Errorf("expected p to be nil, got %v", p)
	}
}

func TestDataSourceHeaders(t *testing.T) {
	cases := []struct {
		desc           string
		input          string
		expectedHeader *common.GeneratedDataHeaders
		expectedToFail bool
	}{
		{
			desc:  "min case: exactly three lines",
			input: "tags,tag1 string,tag2 string2\ncpu,col1,col2\n\n",
			expectedHeader: &common.GeneratedDataHeaders{
				TagTypes:  []string{"string", "string2"},
				TagKeys:   []string{"tag1", "tag2"},
				FieldKeys: map[string][]string{"cpu": {"col1", "col2"}},
			},
		}, {
			desc:  "min case: exactly three lines, tags don't have types",
			input: "tags,tag1 string,tag2 string2\ncpu,col1,col2\n\n",
			expectedHeader: &common.GeneratedDataHeaders{
				TagTypes:  []string{"string", "string2"},
				TagKeys:   []string{"tag1", "tag2"},
				FieldKeys: map[string][]string{"cpu": {"col1", "col2"}},
			},
		},
		{
			desc:           "min case: more than the header 3 lines",
			input:          "tags,tag1,tag2\ncpu,col1,col2\n\nrow1\nrow2\n",
			expectedToFail: true,
		},
		{
			desc:  "multiple tables: more than 3 lines for header",
			input: "tags,tag1 string,tag2 string\ncpu,col1,col2\ndisk,col21,col22\n\n",
			expectedHeader: &common.GeneratedDataHeaders{
				TagTypes: []string{"string", "string"},
				TagKeys:  []string{"tag1", "tag2"},
				FieldKeys: map[string][]string{
					"cpu":  {"col1", "col2"},
					"disk": {"col21", "col22"},
				},
			},
		},
		{
			desc:  "multiple tables: more than 3 lines for header w/ extra",
			input: "tags,tag1 string,tag2 string\ncpu,col1,col2\nmem,col21,col22\n\nrow1\nrow2\n",
			expectedHeader: &common.GeneratedDataHeaders{
				TagTypes: []string{"string", "string"},
				TagKeys:  []string{"tag1", "tag2"},
				FieldKeys: map[string][]string{
					"cpu": {"col1", "col2"},
					"mem": {"col21", "col22"},
				},
			},
		},
		{
			desc:           "too few lines",
			input:          "tags\ncols\n",
			expectedToFail: true,
		},
		{
			desc:           "too fee lines 2",
			input:          "tags\n",
			expectedToFail: true,
		},
	}

	for _, c := range cases {
		br := bufio.NewReader(bytes.NewReader([]byte(c.input)))
		fds := &fileDataSource{scanner: bufio.NewScanner(br)}
		if c.expectedToFail {
			isCalled := false
			fatal = func(fmt string, args ...interface{}) {
				isCalled = true
				log.Printf(fmt, args...)
			}
			fds.Headers()
			if !isCalled {
				t.Errorf("%s: incorrect header parsing must have failed", c.desc)
			}
		} else {
			tableDefs := fds.Headers()
			if fds.headers == nil {
				t.Error("headers should be cached")
			}
			if !reflect.DeepEqual(tableDefs, c.expectedHeader) {
				t.Errorf("incorrect header parsing, got %v; want %v", tableDefs, c.expectedHeader)
			}
		}
	}
}
