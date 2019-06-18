package main

import (
	"bufio"
	"bytes"
	"reflect"
	"testing"
)

func TestDBCreatorReadDataHeader(t *testing.T) {
	cases := []struct {
		desc           string
		input          string
		expectedTables []tableDef
		wantBuffered   int
		expectedToFail bool
	}{
		{
			desc:  "min case: exactly three lines",
			input: "tags,tag1,tag2\ncpu,col1,col2\n\n",
			expectedTables: []tableDef{
				{
					name: "cpu",
					tags: []string{"tag1", "tag2"},
					cols: []string{"col1", "col2"},
				},
			},
			wantBuffered: 0,
		},
		{
			desc:  "min case: more than the header 3 lines",
			input: "tags,tag1,tag2\ncpu,col1,col2\n\nrow1\nrow2\n",
			expectedTables: []tableDef{
				{
					name: "cpu",
					tags: []string{"tag1", "tag2"},
					cols: []string{"col1", "col2"},
				},
			},
			wantBuffered: len([]byte("row1\nrow2\n")),
		},
		{
			desc:  "multiple tables: more than 3 lines for header",
			input: "tags,tag1,tag2\ncpu,col1,col2\ndisk,col21,col22\n\n",
			expectedTables: []tableDef{
				{
					name: "cpu",
					tags: []string{"tag1", "tag2"},
					cols: []string{"col1", "col2"},
				}, {
					name: "disk",
					tags: []string{"tag1", "tag2"},
					cols: []string{"col21", "col22"},
				},
			},
			wantBuffered: 0,
		},
		{
			desc:  "multiple tables: more than 3 lines for header w/ extra",
			input: "tags,tag1,tag2\ncpu,col1,col2\nmem,col21,col22\n\nrow1\nrow2\n",
			expectedTables: []tableDef{
				{
					name: "cpu",
					tags: []string{"tag1", "tag2"},
					cols: []string{"col1", "col2"},
				}, {
					name: "mem",
					tags: []string{"tag1", "tag2"},
					cols: []string{"col21", "col22"},
				},
			},
			wantBuffered: len([]byte("row1\nrow2\n")),
		},
		{
			desc:           "too few lines",
			input:          "tags\ncols\n",
			expectedToFail: true,
		},
		{
			desc:           "no line ender",
			input:          "tags",
			expectedToFail: true,
		},
	}

	for _, c := range cases {
		dbc := &dbCreator{}
		br := bufio.NewReader(bytes.NewReader([]byte(c.input)))
		if c.expectedToFail {
			_, err := dbc.readDataHeader(br)
			if err == nil {
				t.Errorf("%s: incorrect header parsing must have failed", c.desc)
			}
		} else {
			tableDefs, err := dbc.readDataHeader(br)
			if err != nil {
				t.Errorf("%s: incorrect header: %v", c.desc, err)
			}

			for i, tableDef := range tableDefs {
				expectedTableDef := c.expectedTables[i]
				if tableDef.name != expectedTableDef.name {
					t.Errorf("%s: incorrect table name: got\n%s\nwant\n%s",
						c.desc, tableDef.name, expectedTableDef.name)
				}
				if !reflect.DeepEqual(tableDef.tags, expectedTableDef.tags) {
					t.Errorf("%s: incorrect tags: got\n%s\nwant\n%s",
						c.desc, tableDef.tags, expectedTableDef.tags)
				}
				if !reflect.DeepEqual(tableDef.cols, expectedTableDef.cols) {
					t.Errorf("%s: incorrect cols: got\n%s\nwant\n%s\n",
						c.desc, tableDef.cols, expectedTableDef.cols)
				}
				if br.Buffered() != c.wantBuffered {
					t.Errorf("%s: incorrect amt buffered: got\n%d\nwant\n%d",
						c.desc, br.Buffered(), c.wantBuffered)
				}
			}
		}
	}
}
