package main

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"testing"

	"github.com/timescale/tsbs/load"
)

func TestHostnameIndexer(t *testing.T) {
	tagRows := make([]string, 1000, 1000)
	for i := range tagRows {
		tagRows[i] = fmt.Sprintf("host%d,foo", i)
	}
	p := &point{
		hypertable: "foo",
		row:        &insertData{fields: "0.0,1.0,2.0"},
	}

	// single partition check
	indexer := &hostnameIndexer{1}
	for _, r := range tagRows {
		p.row.tags = r
		idx := indexer.GetIndex(load.NewPoint(p))
		if idx != 0 {
			t.Errorf("did not get idx 0 for single partition")
		}
	}

	// multiple partition check
	cases := []uint{2, 10, 100}
	for _, n := range cases {
		parts := uint(n)
		indexer = &hostnameIndexer{parts}
		counts := make([]int, parts, parts)
		verifier := make(map[string]int)
		for _, r := range tagRows {
			p.row.tags = r
			idx := indexer.GetIndex(load.NewPoint(p))
			// check that the partition is not out of bounds
			if idx >= int(parts) {
				t.Errorf("got too large a partition: got %d want %d", idx, parts)
			}
			counts[idx]++
			verifier[r] = idx
		}
		// with 1000 items, very unlikely some partition is empty
		for _, c := range counts {
			if c == 0 {
				t.Errorf("unlikely result of 0 results in a partition for %d partitions", parts)
			}
		}
		// now rerun to verify same tag goes to same idx
		for _, r := range tagRows {
			p.row.tags = r
			idx := indexer.GetIndex(load.NewPoint(p))
			if idx != verifier[r] {
				t.Errorf("indexer returned a different result on %d partitions: got %d want %d", parts, idx, verifier[r])
			}
		}
	}
}

func TestHypertableArr(t *testing.T) {
	f := &factory{}
	ha := f.New().(*hypertableArr)
	if ha.Len() != 0 {
		t.Errorf("hypertableArr not initialized with count 0")
	}
	p := &load.Point{
		Data: &point{
			hypertable: "table1",
			row: &insertData{
				tags:   "t1,t2",
				fields: "0,f1,f2",
			},
		},
	}
	ha.Append(p)
	if ha.Len() != 1 {
		t.Errorf("hypertableArr count is not 1 after first append")
	}
	p = &load.Point{
		Data: &point{
			hypertable: "table2",
			row: &insertData{
				tags:   "t3,t4",
				fields: "1,f3,f4",
			},
		},
	}
	ha.Append(p)
	if ha.Len() != 2 {
		t.Errorf("hypertableArr count is not 2 after 2nd append")
	}
	if len(ha.m) != 2 {
		t.Errorf("hypertableArr does not have 2 different hypertables")
	}
}

func TestDecode(t *testing.T) {
	cases := []struct {
		desc        string
		input       string
		wantPrefix  string
		wantFields  string
		wantTags    string
		shouldFatal bool
	}{
		{
			desc:       "correct input",
			input:      "tags,tag1text,tag2text\ncpu,140,0.0,0.0\n",
			wantPrefix: "cpu",
			wantFields: "140,0.0,0.0",
			wantTags:   "tag1text,tag2text",
		},
		{
			desc:        "incorrect tags prefix",
			input:       "foo,bar,baz\ncpu,140,0.0,0.0\n",
			shouldFatal: true,
		},
		{
			desc:        "missing values line",
			input:       "tags,tag1text,tag2text",
			shouldFatal: true,
		},
	}
	for _, c := range cases {
		br := bufio.NewReader(bytes.NewReader([]byte(c.input)))
		decoder := &decoder{scanner: bufio.NewScanner(br)}
		if c.shouldFatal {
			fmt.Println(c.desc)
			isCalled := false
			fatal = func(fmt string, args ...interface{}) {
				isCalled = true
				log.Printf(fmt, args...)
			}
			_ = decoder.Decode(br)
			if !isCalled {
				t.Errorf("%s: did not call fatal when it should", c.desc)
			}
		} else {
			p := decoder.Decode(br)
			data := p.Data.(*point)
			if data.hypertable != c.wantPrefix {
				t.Errorf("%s: incorrect prefix: got %s want %s", c.desc, data.hypertable, c.wantPrefix)
			}
			if data.row.fields != c.wantFields {
				t.Errorf("%s: incorrect fields: got %s want %s", c.desc, data.row.fields, c.wantFields)
			}
			if data.row.tags != c.wantTags {
				t.Errorf("%s: incorrect tags: got %s want %s", c.desc, data.row.tags, c.wantTags)
			}
		}
	}
}

func TestDecodeEOF(t *testing.T) {
	input := []byte("tags,tag1text,tag2text\ncpu,140,0.0,0.0\n")
	br := bufio.NewReader(bytes.NewReader([]byte(input)))
	decoder := &decoder{scanner: bufio.NewScanner(br)}
	_ = decoder.Decode(br)
	// nothing left, should be EOF
	p := decoder.Decode(br)
	if p != nil {
		t.Errorf("expected p to be nil, got %v", p)
	}
}
