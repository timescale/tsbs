package clickhouse

import (
	"fmt"
	"testing"
)

func TestGenerateTagsTableQuery(t *testing.T) {
	testCases := []struct {
		inTagNames []string
		inTagTypes []string
		out        string
	}{{
		inTagNames: []string{"tag1"},
		inTagTypes: []string{"string"},
		out: "CREATE TABLE tags(\n" +
			"created_date Date     DEFAULT today(),\n" +
			"created_at   DateTime DEFAULT now(),\n" +
			"id           UInt32,\n" +
			"tag1 Nullable(String)" +
			") ENGINE = MergeTree()PARTITION BY toYYYYMM(created_date) PRIMARY KEY id"}, {
		inTagNames: []string{"tag1", "tag2", "tag3", "tag4"},
		inTagTypes: []string{"int32", "int64", "float32", "float64"},
		out: "CREATE TABLE tags(\n" +
			"created_date Date     DEFAULT today(),\n" +
			"created_at   DateTime DEFAULT now(),\n" +
			"id           UInt32,\n" +
			"tag1 Nullable(Int32),\n" +
			"tag2 Nullable(Int64),\n" +
			"tag3 Nullable(Float32),\n" +
			"tag4 Nullable(Float64)" +
			") ENGINE = MergeTree()PARTITION BY toYYYYMM(created_date) PRIMARY KEY id"},
	}
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("tags table for %v", tc.inTagNames), func(t *testing.T) {
			res := generateTagsTableQuery(tc.inTagNames, tc.inTagTypes)
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

	generateTagsTableQuery([]string{"tag"}, []string{})

	t.Fatalf("test should have stopped at this point")
}

func TestGenerateTagsTableQueryPanicOnWrongType(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Errorf("did not panic when should")
		}
	}()

	generateTagsTableQuery([]string{"unknownType"}, []string{"uint32"})

	t.Fatalf("test should have stopped at this point")
}
