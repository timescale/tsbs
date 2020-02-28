package timescaledb

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"testing"
)

func TestDBCreatorInit(t *testing.T) {
	buf := "\n\n\n"
	cases := []struct {
		desc    string
		connStr string
		connDB  string
		want    string
	}{
		{
			desc:    "no dbname replacement needed",
			connStr: "host=localhost user=foo",
			want:    "host=localhost user=foo",
		},
		{
			desc:    "replace once",
			connStr: "host=localhost dbname=test1 user=foo",
			want:    "host=localhost  user=foo",
		},
		{
			desc:    "replace more",
			connStr: "dbname=test2 host=localhost dbname=test1 user=foo dbname=test3",
			want:    "host=localhost  user=foo",
		},
		{
			desc:    "add dbname by specifying a connDB",
			connStr: "host=localhost user=foo",
			connDB:  "bar",
			want:    "dbname=bar host=localhost user=foo",
		},
		{
			desc:    "override once dbname by specifying a connDB",
			connStr: "host=localhost dbname=test1 user=foo",
			connDB:  "bar",
			want:    "dbname=bar host=localhost  user=foo",
		},
		{
			desc:    "override all dbnames by specifying a connDB",
			connStr: "dbname=test2 host=localhost dbname=test1 user=foo dbname=test3",
			connDB:  "bar",
			want:    "dbname=bar host=localhost  user=foo",
		},
	}
	for _, c := range cases {
		br := bufio.NewReader(bytes.NewBufferString(buf))
		dbc := &dbCreator{ds: &fileDataSource{scanner: bufio.NewScanner(br)}, connStr: c.connStr, connDB: c.connDB}
		dbc.initConnectString()
		if got := dbc.connStr; got != c.want {
			t.Errorf("%s: incorrect connstr: got %s want %s", c.desc, got, c.want)
		}
	}
}

func TestDBCreatorGetCreateIndexOnFieldSQL(t *testing.T) {
	hypertable := "htable"
	field := "foo"
	valueTime := "CREATE INDEX ON htable (foo, time DESC)"
	timeValue := "CREATE INDEX ON htable (time DESC, foo)"
	cases := []struct {
		desc        string
		idxType     string
		want        []string
		shouldFatal bool
	}{
		{
			desc:    "no indexes",
			idxType: "",
			want:    []string{},
		},
		{
			desc:    "single TIME-VALUE index",
			idxType: TimeValueIdx,
			want:    []string{timeValue},
		},
		{
			desc:    "single VALUE-TIME index",
			idxType: ValueTimeIdx,
			want:    []string{valueTime},
		},
		{
			desc:    "two indexes",
			idxType: TimeValueIdx + "," + ValueTimeIdx,
			want:    []string{timeValue, valueTime},
		},
		{
			desc:        "bad idxType",
			idxType:     "baz",
			shouldFatal: true,
		},
	}

	for _, c := range cases {
		dbc := &dbCreator{}
		if c.shouldFatal {
			isCalled := false
			fatal = func(fmt string, args ...interface{}) {
				isCalled = true
				log.Printf(fmt, args...)
			}
			dbc.getCreateIndexOnFieldCmds(hypertable, field, c.idxType)
			if !isCalled {
				t.Errorf("%s: did not call fatal when it should", c.desc)
			}
		} else {
			cmds := dbc.getCreateIndexOnFieldCmds(hypertable, field, c.idxType)
			if len(cmds) != len(c.want) {
				t.Errorf("%s: incorrect cmds length: got %d want %d", c.desc, len(cmds), len(c.want))
			}
			for i, cmd := range cmds {
				if cmd != c.want[i] {
					t.Errorf("%s: incorrect cmd at idx %d: got %s want %s", c.desc, i, cmd, c.want[i])
				}
			}
		}
	}
}

func TestDBCreatorGetFieldAndIndexDefinitions(t *testing.T) {
	cases := []struct {
		desc            string
		tableName       string
		columns         []string
		fieldIndexCount int
		inTableTag      bool
		wantFieldDefs   []string
		wantIndexDefs   []string
	}{
		{
			desc:            "all field indexes",
			tableName:       "cpu",
			columns:         []string{"usage_user", "usage_system", "usage_idle", "usage_nice"},
			fieldIndexCount: -1,
			inTableTag:      false,
			wantFieldDefs:   []string{"usage_user DOUBLE PRECISION", "usage_system DOUBLE PRECISION", "usage_idle DOUBLE PRECISION", "usage_nice DOUBLE PRECISION"},
			wantIndexDefs:   []string{"CREATE INDEX ON cpu (usage_user, time DESC)", "CREATE INDEX ON cpu (usage_system, time DESC)", "CREATE INDEX ON cpu (usage_idle, time DESC)", "CREATE INDEX ON cpu (usage_nice, time DESC)"},
		},
		{
			desc:            "no field indexes",
			tableName:       "cpu",
			columns:         []string{"usage_user", "usage_system", "usage_idle", "usage_nice"},
			fieldIndexCount: 0,
			inTableTag:      false,
			wantFieldDefs:   []string{"usage_user DOUBLE PRECISION", "usage_system DOUBLE PRECISION", "usage_idle DOUBLE PRECISION", "usage_nice DOUBLE PRECISION"},
			wantIndexDefs:   []string{},
		},
		{
			desc:            "no field indexes, in table tag",
			tableName:       "cpu",
			columns:         []string{"usage_user", "usage_system", "usage_idle", "usage_nice"},
			fieldIndexCount: 0,
			inTableTag:      true,
			wantFieldDefs:   []string{"hostname TEXT", "usage_user DOUBLE PRECISION", "usage_system DOUBLE PRECISION", "usage_idle DOUBLE PRECISION", "usage_nice DOUBLE PRECISION"},
			wantIndexDefs:   []string{},
		},
		{
			desc:            "one field index",
			tableName:       "cpu",
			columns:         []string{"usage_user", "usage_system", "usage_idle", "usage_nice"},
			fieldIndexCount: 1,
			inTableTag:      false,
			wantFieldDefs:   []string{"usage_user DOUBLE PRECISION", "usage_system DOUBLE PRECISION", "usage_idle DOUBLE PRECISION", "usage_nice DOUBLE PRECISION"},
			wantIndexDefs:   []string{"CREATE INDEX ON cpu (usage_user, time DESC)"},
		},
		{
			desc:            "two field indexes",
			tableName:       "cpu",
			columns:         []string{"usage_user", "usage_system", "usage_idle", "usage_nice"},
			fieldIndexCount: 2,
			inTableTag:      false,
			wantFieldDefs:   []string{"usage_user DOUBLE PRECISION", "usage_system DOUBLE PRECISION", "usage_idle DOUBLE PRECISION", "usage_nice DOUBLE PRECISION"},
			wantIndexDefs:   []string{"CREATE INDEX ON cpu (usage_user, time DESC)", "CREATE INDEX ON cpu (usage_system, time DESC)"},
		},
	}

	for _, c := range cases {
		// Set the global in-table-tag flag based on the test case
		// Initialize global cache
		tableCols[tagsKey] = []string{}
		tableCols[tagsKey] = append(tableCols[tagsKey], "hostname")
		dbc := &dbCreator{opts: &LoadingOptions{
			InTableTag:      c.inTableTag,
			FieldIndexCount: c.fieldIndexCount,
		}}
		fieldDefs, indexDefs := dbc.getFieldAndIndexDefinitions(c.tableName, c.columns)
		for i, fieldDef := range fieldDefs {
			if fieldDef != c.wantFieldDefs[i] {
				t.Errorf("%s: incorrect fieldDef at idx %d: got %s want %s", c.desc, i, fieldDef, c.wantFieldDefs[i])
			}
		}
		for i, indexDef := range indexDefs {
			if indexDef != c.wantIndexDefs[i] {
				t.Errorf("%s: incorrect indexDef at idx %d: got %s want %s", c.desc, i, indexDef, c.wantIndexDefs[i])
			}
		}
	}
}

func TestExtractTagNamesAndTypes(t *testing.T) {
	names, types := extractTagNamesAndTypes([]string{"tag1 type1", "tag2 type2"})
	if names[0] != "tag1" || names[1] != "tag2" {
		t.Errorf("expected tag names tag1 and tag2, got: %v", names)
	}
	if types[0] != "type1" || types[1] != "type2" {
		t.Errorf("expected tag types type1 and type2, got: %v", types)

	}
}
func TestGenerateTagsTableQuery(t *testing.T) {
	testCases := []struct {
		in  []string
		inT []string
		out string
	}{
		{
			in:  []string{"tag1"},
			inT: []string{"string"},
			out: "CREATE TABLE tags(id SERIAL PRIMARY KEY, tag1 TEXT)",
		}, {
			in:  []string{"tag1", "tag2", "tag3", "tag4"},
			inT: []string{"int32", "int64", "float32", "float64"},
			out: "CREATE TABLE tags(id SERIAL PRIMARY KEY, tag1 INTEGER, tag2 BIGINT," +
				" tag3 FLOAT, tag4 DOUBLE PRECISION)",
		},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("Generate tags table for: %s", tc.in), func(t *testing.T) {
			res := generateTagsTableQuery(tc.in, tc.inT)
			if res != tc.out {
				t.Errorf("tags table not properly created\n expected: %s\n got: %s\n", tc.out, res)
			}
		})
	}
}

func TestGenerateTagsTableQueryPanicOnWrongType(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Errorf("did not panic when should")
		}
	}()

	generateTagsTableQuery([]string{"tag"}, []string{"uint32"})

	t.Fatalf("test should have stopped at this point")
}
