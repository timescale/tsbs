package timescaledb

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"strings"
	"testing"

	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
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
		idx := indexer.GetIndex(data.NewLoadedPoint(p))
		if idx != 0 {
			t.Errorf("did not get idx 0 for single partition")
		}
	}

	// multiple partition check
	cases := []uint{2, 10, 100}
	for _, n := range cases {
		parts := n
		indexer = &hostnameIndexer{parts}
		counts := make([]int, parts, parts)
		verifier := make(map[string]uint)
		for _, r := range tagRows {
			p.row.tags = r
			idx := indexer.GetIndex(data.NewLoadedPoint(p))
			// check that the partition is not out of bounds
			if idx >= parts {
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
			idx := indexer.GetIndex(data.NewLoadedPoint(p))
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
	p := data.LoadedPoint{
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
	p = data.LoadedPoint{
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
		dataSource := &fileDataSource{
			scanner: bufio.NewScanner(br),
			headers: &common.GeneratedDataHeaders{},
		}
		if c.shouldFatal {
			fmt.Println(c.desc)
			isCalled := false
			fatal = func(fmt string, args ...interface{}) {
				isCalled = true
				log.Printf(fmt, args...)
			}
			_ = dataSource.NextItem()
			if !isCalled {
				t.Errorf("%s: did not call fatal when it should", c.desc)
			}
		} else {
			p := dataSource.NextItem()
			newpoint := p.Data.(*point)
			if newpoint.hypertable != c.wantPrefix {
				t.Errorf("%s: incorrect prefix: got %s want %s", c.desc, newpoint.hypertable, c.wantPrefix)
			}
			if newpoint.row.fields != c.wantFields {
				t.Errorf("%s: incorrect fields: got %s want %s", c.desc, newpoint.row.fields, c.wantFields)
			}
			if newpoint.row.tags != c.wantTags {
				t.Errorf("%s: incorrect tags: got %s want %s", c.desc, newpoint.row.tags, c.wantTags)
			}
		}
	}
}

func TestDecodeEOF(t *testing.T) {
	input := []byte("tags,tag1text,tag2text\ncpu,140,0.0,0.0\n")
	br := bufio.NewReader(bytes.NewReader(input))
	decoder := &fileDataSource{headers: &common.GeneratedDataHeaders{}, scanner: bufio.NewScanner(br)}
	_ = decoder.NextItem()
	// nothing left, should be EOF
	p := decoder.NextItem()
	if p.Data != nil {
		t.Errorf("expected p to be nil, got %v", p)
	}
}

func TestFileDataSourceHeaders(t *testing.T) {
	cases := []struct {
		desc        string
		input       string
		wantTags    string
		wantTypes   string
		wantCols    map[string]string
		shouldFatal bool
	}{
		{
			desc:      "min case: exactly three lines",
			input:     "tags,tag1 tag,tag2 tag\ncols,col1,col2\n\n",
			wantTags:  "tag1,tag2",
			wantTypes: "tag,tag",
			wantCols:  map[string]string{"cols": "col1,col2"},
		},
		{
			desc:      "min case: more than the header 3 lines",
			input:     "tags,tag1 tag,tag2 tag2\ncols,col1,col2\n\nrow1\nrow2\n",
			wantTags:  "tag1,tag2",
			wantTypes: "tag,tag2",
			wantCols:  map[string]string{"cols": "col1,col2"},
		},
		{
			desc:      "multiple tables: more than 3 lines for header",
			input:     "tags,tag1 int,tag2 mint\ncols,col1,col2\ncols2,col21,col22\n\n",
			wantTypes: "int,mint",
			wantTags:  "tag1,tag2",
			wantCols:  map[string]string{"cols": "col1,col2", "cols2": "col21,col22"},
		},
		{
			desc:      "multiple tables: more than 3 lines for header w/ extra",
			input:     "tags,tag1 tagT,tag2 tag2\ncols,col1,col2\ncols2,col21,col22\n\nrow1\nrow2\n",
			wantTags:  "tag1,tag2",
			wantTypes: "tagT,tag2",
			wantCols:  map[string]string{"cols": "col1,col2", "cols2": "col21,col22"},
		},
		{
			desc:        "too few lines",
			input:       "tags\ncols\n",
			shouldFatal: true,
		},
		{
			desc:        "no line ender",
			input:       "tags",
			shouldFatal: true,
		},
	}

	for _, c := range cases {
		br := bufio.NewReader(bytes.NewReader([]byte(c.input)))
		ds := &fileDataSource{
			scanner: bufio.NewScanner(br),
		}

		if c.shouldFatal {
			isCalled := false
			fatal = func(fmt string, args ...interface{}) {
				isCalled = true
				log.Printf(fmt, args...)
			}
			ds.Headers()
			if !isCalled {
				t.Errorf("%s: did not call fatal when it should", c.desc)
			}
		} else {
			headers := ds.Headers()

			gotKeys := strings.Join(headers.TagKeys, ",")
			if gotKeys != c.wantTags {
				t.Errorf("%s: incorrect tags: got\n%s\nwant\n%s", c.desc, gotKeys, c.wantTags)
			}
			gotKeyTypes := strings.Join(headers.TagTypes, ",")
			if gotKeyTypes != c.wantTypes {
				t.Errorf("%s: incorrect types: got\n%s\nwant\n%s", c.desc, gotKeyTypes, c.wantTypes)
			}

			if len(headers.FieldKeys) != len(c.wantCols) {
				t.Errorf("%s: incorrect cols len: got %d want %d", c.desc, len(headers.FieldKeys), len(c.wantCols))
			}
			for table, columns := range headers.FieldKeys {
				got := strings.Join(columns, ",")
				if got != c.wantCols[table] {
					t.Errorf("%s: cols for table %s, incorrect: got\n%s\nwant\n%s\n", c.desc, table, got, c.wantCols[table])
				}
			}
		}
	}
}
