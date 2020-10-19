package main

import (
	"testing"

	"github.com/timescale/tsbs/pkg/data/usecases/common"
)

func TestDBCreatorReadDataHeader(t *testing.T) {
	cases := []struct {
		desc           string
		input          *common.GeneratedDataHeaders
		expectedTables map[string]tableDef
		expectedToFail bool
	}{
		{
			desc: "min case",
			input: &common.GeneratedDataHeaders{
				TagTypes:  nil,
				TagKeys:   []string{"tag1", "tag2"},
				FieldKeys: map[string][]string{"cpu": {"col1", "col2"}},
			},
			expectedTables: map[string]tableDef{
				"cpu": {
					name: "cpu",
					tags: []string{"tag1", "tag2"},
					cols: []string{"col1", "col2"},
				},
			},
		}, {
			desc: "multiple tables",
			input: &common.GeneratedDataHeaders{
				TagTypes:  nil,
				TagKeys:   []string{"tag1", "tag2"},
				FieldKeys: map[string][]string{"cpu": {"col1", "col2"}, "disk": {"col21", "col22"}},
			},
			expectedTables: map[string]tableDef{
				"cpu": {
					name: "cpu",
					tags: []string{"tag1", "tag2"},
					cols: []string{"col1", "col2"},
				}, "disk": {
					name: "disk",
					tags: []string{"tag1", "tag2"},
					cols: []string{"col21", "col22"},
				},
			},
		},
		{
			desc: "no field keys no table defs",
			input: &common.GeneratedDataHeaders{
				TagTypes:  nil,
				TagKeys:   []string{"tag1", "tag2"},
				FieldKeys: nil,
			},
			expectedToFail: false,
			expectedTables: map[string]tableDef{},
		},
	}

	for _, c := range cases {
		dbc := &dbCreator{}
		if c.expectedToFail {
			_, err := dbc.readDataHeader(c.input)
			if err == nil {
				t.Errorf("%s: incorrect header, must have failed", c.desc)
			}
		} else {
			tableDefs, err := dbc.readDataHeader(c.input)
			if err != nil {
				t.Errorf("%s: incorrect header: %v", c.desc, err)
			}

			for _, tableDef := range tableDefs {
				expectedTableDef := c.expectedTables[tableDef.name]
				if expectedTableDef.name == "" {
					t.Errorf("expected tables didn't contain returned def %s", tableDef.name)
				}
				if tableDef.name != expectedTableDef.name {
					t.Errorf("%s: incorrect table name: got\n%s\nwant\n%s",
						c.desc, tableDef.name, expectedTableDef.name)
				}
				if !arrEq(tableDef.tags, expectedTableDef.tags) {
					t.Errorf("%s: incorrect tags: got\n%s\nwant\n%s",
						c.desc, tableDef.tags, expectedTableDef.tags)
				}
				if !arrEq(tableDef.cols, expectedTableDef.cols) {
					t.Errorf("%s: incorrect cols: got\n%s\nwant\n%s\n",
						c.desc, tableDef.cols, expectedTableDef.cols)
				}
			}
		}
	}
}

func arrEq(a []string, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	if a == nil && b == nil {
		return true
	}
	for i, x := range a {
		if b[i] != x {
			return false
		}
	}
	return true
}
