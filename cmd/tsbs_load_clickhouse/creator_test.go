package main

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"testing"
)

func TestDBCreatorReadDataHeader(t *testing.T) {
	cases := []struct {
		desc         string
		input        string
		wantTags     string
		wantCols     []string
		wantBuffered int
		shouldFatal  bool
	}{
		{
			desc:         "min case: exactly three lines",
			input:        "tags,tag1 string,tag2 float32\ncols,col1,col2\n\n",
			wantTags:     "tags,tag1 string,tag2 float32",
			wantCols:     []string{"cols,col1,col2"},
			wantBuffered: 0,
		},
		{
			desc:         "min case: more than the header 3 lines",
			input:        "tags,tag1 string,tag2 string\ncols,col1,col2\n\nrow1\nrow2\n",
			wantTags:     "tags,tag1 string,tag2 string",
			wantCols:     []string{"cols,col1,col2"},
			wantBuffered: len([]byte("row1\nrow2\n")),
		},
		{
			desc:         "multiple tables: more than 3 lines for header",
			input:        "tags,tag1 int32,tag2 int64\ncols,col1,col2\ncols2,col21,col22\n\n",
			wantTags:     "tags,tag1 int32,tag2 int64",
			wantCols:     []string{"cols,col1,col2", "cols2,col21,col22"},
			wantBuffered: 0,
		},
		{
			desc:         "multiple tables: more than 3 lines for header w/ extra",
			input:        "tags,tag1 tag,tag2 tag2\ncols,col1,col2\ncols2,col21,col22\n\nrow1\nrow2\n",
			wantTags:     "tags,tag1 tag,tag2 tag2",
			wantCols:     []string{"cols,col1,col2", "cols2,col21,col22"},
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
		dbc := &dbCreator{}
		br := bufio.NewReader(bytes.NewReader([]byte(c.input)))
		if c.shouldFatal {
			isCalled := false
			fatal = func(fmt string, args ...interface{}) {
				isCalled = true
				log.Printf(fmt, args...)
			}
			dbc.readDataHeader(br)
			if !isCalled {
				t.Errorf("%s: did not call fatal when it should", c.desc)
			}
		} else {
			dbc.readDataHeader(br)
			if dbc.tags != c.wantTags {
				t.Errorf("%s: incorrect tags: got\n%s\nwant\n%s", c.desc, dbc.tags, c.wantTags)
			}
			if len(dbc.cols) != len(c.wantCols) {
				t.Errorf("%s: incorrect cols len: got %d want %d", c.desc, len(dbc.cols), len(c.wantCols))
			}
			for i := range dbc.cols {
				if got := dbc.cols[i]; got != c.wantCols[i] {
					t.Errorf("%s: cols row %d incorrect: got\n%s\nwant\n%s\n", c.desc, i, got, c.wantCols[i])
				}
			}
			if br.Buffered() != c.wantBuffered {
				t.Errorf("%s: incorrect amt buffered: got\n%d\nwant\n%d", c.desc, br.Buffered(), c.wantBuffered)
			}
		}
	}
}

func TestGenerateTagsTableQuery(t *testing.T) {
	testCases := []struct {
		in  []string
		out string
	}{{
		in: []string{"tag1 string"},
		out: "CREATE TABLE tags(\n" +
			"created_date Date     DEFAULT today(),\n" +
			"created_at   DateTime DEFAULT now(),\n" +
			"id           UInt32,\n" +
			"tag1 String" +
			") ENGINE = MergeTree(created_date, (id), 8192)"}, {
		in: []string{"tag1 int32", "tag2 int64", "tag3 float32", "tag4 float64"},
		out: "CREATE TABLE tags(\n" +
			"created_date Date     DEFAULT today(),\n" +
			"created_at   DateTime DEFAULT now(),\n" +
			"id           UInt32,\n" +
			"tag1 Int32,\n" +
			"tag2 Int64,\n" +
			"tag3 Float32,\n" +
			"tag4 Float64" +
			") ENGINE = MergeTree(created_date, (id), 8192)"},
	}
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("tags table for %v", tc.in), func(t *testing.T) {
			res := generateTagsTableQuery(tc.in)
			if res != tc.out {
				t.Errorf("unexpected result.\nexpected: %s\ngot: %s", tc.out, res)
			}
		})
	}
}

func TestGenerateTagsTableQueryPanicOnWrongFormat(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Errorf("did not panic when should")
		}
	}()

	generateTagsTableQuery([]string{"tagWithoutType"})

	t.Fatalf("test should have stopped at this point")
}

func TestGenerateTagsTableQueryPanicOnWrongType(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Errorf("did not panic when should")
		}
	}()

	generateTagsTableQuery([]string{"unknownType uint32"})

	t.Fatalf("test should have stopped at this point")
}
