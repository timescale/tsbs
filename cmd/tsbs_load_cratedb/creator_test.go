package main

import (
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"reflect"
	"testing"
)

func TestDBCreatorReadDataHeader(t *testing.T) {
	cases := []struct {
		desc           string
		input          *common.GeneratedDataHeaders
		expectedTables []tableDef
		expectedToFail bool
	}{
		{
			desc: "min case",
			input: &common.GeneratedDataHeaders{
				TagTypes:  nil,
				TagKeys:   []string{"tag1", "tag2"},
				FieldKeys: map[string][]string{"cpu": {"col1", "col2"}},
			},
			expectedTables: []tableDef{
				{
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
		},
		{
			desc: "no field keys no table defs",
			input: &common.GeneratedDataHeaders{
				TagTypes:  nil,
				TagKeys:   []string{"tag1", "tag2"},
				FieldKeys: nil,
			},
			expectedToFail: false,
			expectedTables: []tableDef{},
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
			}
		}
	}
}
