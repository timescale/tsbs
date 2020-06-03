package clickhouse

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"testing"

	"github.com/timescale/tsbs/pkg/data"
)

func TestGetConnectString(t *testing.T) {
	wantHost := "localhost"
	wantUser := "default"
	wantPassword := ""
	wantDB := "benchmark"
	want := fmt.Sprintf("tcp://%s:9000?username=%s&password=%s&database=%s", wantHost, wantUser, wantPassword, wantDB)

	connStr := getConnectString(&ClickhouseConfig{
		Host:     wantHost,
		User:     wantUser,
		Password: wantPassword,
		DbName:   wantDB,
	},
		true)
	if connStr != want {
		t.Errorf("incorrect connect string: got %s want %s", connStr, want)
	}
}

func TestHypertableArr(t *testing.T) {
	f := &factory{}
	ha := f.New().(*tableArr)
	if ha.Len() != 0 {
		t.Errorf("tableArr not initialized with count 0")
	}
	p := data.LoadedPoint{
		Data: &point{
			table: "table1",
			row: &insertData{
				tags:   "t1,t2",
				fields: "0,f1,f2",
			},
		},
	}
	ha.Append(p)
	if ha.Len() != 1 {
		t.Errorf("tableArr count is not 1 after first append")
	}
	p = data.LoadedPoint{
		Data: &point{
			table: "table2",
			row: &insertData{
				tags:   "t3,t4",
				fields: "1,f3,f4",
			},
		},
	}
	ha.Append(p)
	if ha.Len() != 2 {
		t.Errorf("tableArr count is not 2 after 2nd append")
	}
	if len(ha.m) != 2 {
		t.Errorf("tableArr does not have 2 different hypertables")
	}
}

func TestNextItem(t *testing.T) {
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
		dataSource := &fileDataSource{scanner: bufio.NewScanner(br)}
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
			data := p.Data.(*point)
			if data.table != c.wantPrefix {
				t.Errorf("%s: incorrect prefix: got %s want %s", c.desc, data.table, c.wantPrefix)
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
	dataSource := &fileDataSource{scanner: bufio.NewScanner(br)}
	_ = dataSource.NextItem()
	// nothing left, should be EOF
	p := dataSource.NextItem()
	if p.Data != nil {
		t.Errorf("expected p to be nil, got %v", p)
	}
}

func TestHeaders(t *testing.T) {
	cases := []struct {
		desc         string
		input        string
		wantTags     []string
		wantCols     map[string][]string
		wantTypes    []string
		shouldFatal  bool
		wantBuffered int
	}{
		{
			desc:         "min case: exactly three lines",
			input:        "tags,tag1 string,tag2 float32\ncols,col1,col2\n\n",
			wantTags:     []string{"tag1", "tag2"},
			wantCols:     map[string][]string{"cols": {"col1", "col2"}},
			wantTypes:    []string{"string", "float32"},
			wantBuffered: 0,
		},
		{
			desc:         "min case: more than the header 3 lines",
			input:        "tags,tag1 string,tag2 string\ncols,col1,col2\n\nrow1\nrow2\n",
			wantTags:     []string{"tag1", "tag2"},
			wantTypes:    []string{"string", "string"},
			wantCols:     map[string][]string{"cols": {"col1", "col2"}},
			wantBuffered: len([]byte("row1\nrow2\n")),
		},
		{
			desc:         "multiple tables: more than 3 lines for header",
			input:        "tags,tag1 int32,tag2 int64\ncols,col1,col2\ncols2,col21,col22\n\n",
			wantTags:     []string{"tag1", "tag2"},
			wantTypes:    []string{"int32", "int64"},
			wantCols:     map[string][]string{"cols": {"col1", "col2"}, "cols2": {"col21", "col22"}},
			wantBuffered: 0,
		},
		{
			desc:         "multiple tables: more than 3 lines for header w/ extra",
			input:        "tags,tag1 tag,tag2 tag2\ncols,col1,col2\ncols2,col21,col22\n\nrow1\nrow2\n",
			wantTags:     []string{"tag1", "tag2"},
			wantTypes:    []string{"tag", "tag2"},
			wantCols:     map[string][]string{"cols": {"col1", "col2"}, "cols2": {"col21", "col22"}},
			wantBuffered: len([]byte("row1\nrow2\n")),
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
		dataSource := &fileDataSource{bufio.NewScanner(br), nil}
		if c.shouldFatal {
			isCalled := false
			fatal = func(fmt string, args ...interface{}) {
				isCalled = true
				log.Printf(fmt, args...)
			}
			dataSource.Headers()
			if !isCalled {
				t.Errorf("%s: did not call fatal when it should", c.desc)
			}
		} else {
			headers := dataSource.Headers()
			if !strArrEq(headers.TagKeys, c.wantTags) {
				t.Errorf("%s: incorrect tags: got\n%v\nwant\n%v", c.desc, headers.TagKeys, c.wantTags)
			}
			if !strArrEq(headers.TagTypes, c.wantTypes) {
				t.Errorf("%s: incorrect tag types: got\n%v\nwant\n%v", c.desc, headers.TagTypes, c.wantTypes)
			}

			if len(headers.FieldKeys) != len(c.wantCols) {
				t.Errorf("%s: incorrect cols len: got %d want %d", c.desc, len(headers.FieldKeys), len(c.wantCols))
			}
			for key, got := range headers.FieldKeys {
				want := c.wantCols[key]
				if !strArrEq(got, want) {
					t.Errorf("%s: cols row incorrect: got\n%v\nwant\n%v\n", c.desc, got, want)
				}
			}
		}
	}
}

func strArrEq(a []string, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i, aa := range a {
		if aa != b[i] {
			return false
		}
	}
	return true
}
