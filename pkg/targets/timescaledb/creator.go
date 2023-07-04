package timescaledb

import (
	"database/sql"
	"fmt"
	"log"
	"regexp"
	"strings"
	"time"

	"github.com/timescale/tsbs/pkg/targets"

	_ "github.com/jackc/pgx/v4/stdlib"
)

const (
	tagsKey      = "tags"
	TimeValueIdx = "TIME-VALUE"
	ValueTimeIdx = "VALUE-TIME"
)

// allows for testing
var fatal = log.Fatalf

var tableCols = make(map[string][]string)

type dbCreator struct {
	driver  string
	ds      targets.DataSource
	connStr string
	connDB  string
	opts    *LoadingOptions
}

func (d *dbCreator) Init() {
	// read the headers before all else
	d.ds.Headers()
	d.initConnectString()
}

func (d *dbCreator) initConnectString() {
	// Needed to connect to user's database in order to drop/create db-name database
	re := regexp.MustCompile(`(dbname)=\S*\b`)
	d.connStr = strings.TrimSpace(re.ReplaceAllString(d.connStr, ""))

	if d.connDB != "" {
		d.connStr = fmt.Sprintf("dbname=%s %s", d.connDB, d.connStr)
	}
}

// MustConnect connects or exits on errors
func MustConnect(dbType, connStr string) *sql.DB {
	db, err := sql.Open(dbType, connStr)
	if err != nil {
		panic(err)
	}
	return db
}

func (d *dbCreator) DBExists(dbName string) bool {
	db := MustConnect(d.driver, d.connStr)
	defer db.Close()
	r := MustQuery(db, "SELECT 1 from pg_database WHERE datname = $1", dbName)
	defer r.Close()
	return r.Next()
}

func (d *dbCreator) RemoveOldDB(dbName string) error {
	db := MustConnect(d.driver, d.connStr)
	defer db.Close()
	MustExec(db, "DROP DATABASE IF EXISTS "+dbName)
	return nil
}

func (d *dbCreator) CreateDB(dbName string) error {
	db := MustConnect(d.driver, d.connStr)
	MustExec(db, "CREATE DATABASE "+dbName)
	db.Close()
	return nil
}

func (d *dbCreator) PostCreateDB(dbName string) error {
	dbBench := MustConnect(d.driver, d.opts.GetConnectString(dbName))
	defer dbBench.Close()

	headers := d.ds.Headers()
	tagNames := headers.TagKeys
	tagTypes := headers.TagTypes
	if d.opts.CreateMetricsTable {
		createTagsTable(dbBench, tagNames, tagTypes, d.opts.UseJSON)
	}
	// tableCols is a global map. Globally cache the available tags
	tableCols[tagsKey] = tagNames
	// tagTypes holds the type of each tag value (as strings from Go types (string, float32...))
	d.opts.TagColumnTypes = tagTypes

	// Each table is defined in the dbCreator 'cols' list. The definition consists of a
	// comma separated list of the table name followed by its columns. Iterate over each
	// definition to update our global cache and create the requisite tables and indexes
	for tableName, columns := range headers.FieldKeys {
		// tableCols is a global map. Globally cache the available columns for the given table
		tableCols[tableName] = columns
		fieldDefs, indexDefs := d.getFieldAndIndexDefinitions(tableName, columns)
		if d.opts.CreateMetricsTable {
			d.createTableAndIndexes(dbBench, tableName, fieldDefs, indexDefs)
		} else {
			// If not creating table, wait for another client to set it up
			i := 0
			checkTableQuery := fmt.Sprintf("SELECT * FROM pg_tables WHERE tablename = '%s'", tableName)
			r := MustQuery(dbBench, checkTableQuery)
			for !r.Next() {
				time.Sleep(100 * time.Millisecond)
				i += 1
				if i == 600 {
					return fmt.Errorf("expected table not created after one minute of waiting")
				}
				r = MustQuery(dbBench, checkTableQuery)
			}
		}
	}
	return nil
}

// getFieldAndIndexDefinitions iterates over a list of table columns, populating lists of
// definitions for each desired field and index. Returns separate lists of fieldDefs and indexDefs
func (d *dbCreator) getFieldAndIndexDefinitions(tableName string, columns []string) ([]string, []string) {
	var fieldDefs []string
	var indexDefs []string
	var allCols []string

	partitioningField := tableCols[tagsKey][0]
	// If the user has specified that we should partition on the primary tags key, we
	// add that to the list of columns to create
	if d.opts.InTableTag {
		allCols = append(allCols, partitioningField)
	}

	allCols = append(allCols, columns...)
	extraCols := 0 // set to 1 when hostname is kept in-table
	for idx, field := range allCols {
		if len(field) == 0 {
			continue
		}
		fieldType := "DOUBLE PRECISION"
		idxType := d.opts.FieldIndex
		// This condition handles the case where we keep the primary tag key in the table
		// and partition on it. Since under the current implementation this tag is always
		// hostname, we set it to a TEXT field instead of DOUBLE PRECISION
		if d.opts.InTableTag && idx == 0 {
			fieldType = "TEXT"
			idxType = ""
			extraCols = 1
		}

		fieldDefs = append(fieldDefs, fmt.Sprintf("%s %s", field, fieldType))
		// If the user specifies indexes on additional fields, add them to
		// our index definitions until we've reached the desired number of indexes
		if d.opts.FieldIndexCount == -1 || idx < (d.opts.FieldIndexCount+extraCols) {
			indexDefs = append(indexDefs, d.getCreateIndexOnFieldCmds(tableName, field, idxType)...)
		}
	}
	return fieldDefs, indexDefs
}

// createTableAndIndexes takes a list of field and index definitions for a given tableName and constructs
// the necessary table, index, and potential hypertable based on the user's settings
func (d *dbCreator) createTableAndIndexes(dbBench *sql.DB, tableName string, fieldDefs []string, indexDefs []string) {
	// We default to the tags_id column unless users are creating the
	// name/hostname column in the time-series table for multi-node
	// testing. For distributed queries, pushdown of JOINs is not yet
	// supported.
	var partitionColumn string = "tags_id"

	if d.opts.InTableTag {
		partitionColumn = tableCols[tagsKey][0]
	}

	MustExec(dbBench, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	MustExec(dbBench, fmt.Sprintf("CREATE TABLE %s (time timestamptz, tags_id integer, %s, additional_tags JSONB DEFAULT NULL)", tableName, strings.Join(fieldDefs, ",")))
	if d.opts.PartitionIndex {
		MustExec(dbBench, fmt.Sprintf("CREATE INDEX ON %s(%s, \"time\" DESC)", tableName, partitionColumn))
	}

	// Only allow one or the other, it's probably never right to have both.
	// Experimentation suggests (so far) that for 100k devices it is better to
	// use --time-partition-index for reduced index lock contention.
	if d.opts.TimePartitionIndex {
		MustExec(dbBench, fmt.Sprintf("CREATE INDEX ON %s(\"time\" DESC, %s)", tableName, partitionColumn))
	} else if d.opts.TimeIndex {
		MustExec(dbBench, fmt.Sprintf("CREATE INDEX ON %s(\"time\" DESC)", tableName))
	}

	for _, indexDef := range indexDefs {
		MustExec(dbBench, indexDef)
	}

	if d.opts.UseHypertable {
		var creationCommand string = "create_hypertable"
		var partitionsOption string = "replication_factor => NULL"

		MustExec(dbBench, "CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE")

		// Replication factor determines whether we create a distributed hypertable
		// or not. If it is unset or zero, then we will create a regular
		// hypertable with no partitions.
		//
		// If replication factor is greater >= 1, we assume there are at least multiple
		// data nodes. We currently use `create_hypertable` for both statements, the
		// default behavior is to create a distributed hypertable if `replication_factor`
		// is >= 1

		// We assume a single partition hypertable. This provides an option to test
		// partitioning on regular hypertables
		if d.opts.NumberPartitions > 0 {
			partitionsOption = fmt.Sprintf("partitioning_column => '%s'::name, number_partitions => %v::smallint", partitionColumn, d.opts.NumberPartitions)
		}

		if d.opts.ReplicationFactor > 0 {
			// This gives us a future option of testing the impact of
			// multi-node replication across data nodes
			partitionsOption = fmt.Sprintf("partitioning_column => '%s'::name, replication_factor => %v::smallint", partitionColumn, d.opts.ReplicationFactor)
		}

		MustExec(dbBench,
			fmt.Sprintf("SELECT %s('%s'::regclass, 'time'::name, %s, chunk_time_interval => %d, create_default_indexes=>FALSE)",
				creationCommand, tableName, partitionsOption, d.opts.ChunkTime.Nanoseconds()/1000))
	}
}

func (d *dbCreator) getCreateIndexOnFieldCmds(hypertable, field, idxType string) []string {
	var ret []string
	for _, idx := range strings.Split(idxType, ",") {
		if idx == "" {
			continue
		}

		indexDef := ""
		if idx == TimeValueIdx {
			indexDef = fmt.Sprintf("(time DESC, %s)", field)
		} else if idx == ValueTimeIdx {
			indexDef = fmt.Sprintf("(%s, time DESC)", field)
		} else {
			fatal("Unknown index type %v", idx)
		}

		ret = append(ret, fmt.Sprintf("CREATE INDEX ON %s %s", hypertable, indexDef))
	}
	return ret
}

func createTagsTable(db *sql.DB, tagNames, tagTypes []string, useJSON bool) {
	MustExec(db, "DROP TABLE IF EXISTS tags")
	if useJSON {
		MustExec(db, "CREATE TABLE tags(id SERIAL PRIMARY KEY, tagset JSONB)")
		MustExec(db, "CREATE UNIQUE INDEX uniq1 ON tags(tagset)")
		MustExec(db, "CREATE INDEX idxginp ON tags USING gin (tagset jsonb_path_ops);")
		return
	}

	MustExec(db, generateTagsTableQuery(tagNames, tagTypes))
	MustExec(db, fmt.Sprintf("CREATE UNIQUE INDEX uniq1 ON tags(%s)", strings.Join(tagNames, ",")))
	MustExec(db, fmt.Sprintf("CREATE INDEX ON tags(%s)", tagNames[0]))
}

func generateTagsTableQuery(tagNames, tagTypes []string) string {
	tagColumnDefinitions := make([]string, len(tagNames))
	for i, tagName := range tagNames {
		pgType := serializedTypeToPgType(tagTypes[i])
		tagColumnDefinitions[i] = fmt.Sprintf("%s %s", tagName, pgType)
	}

	cols := strings.Join(tagColumnDefinitions, ", ")
	return fmt.Sprintf("CREATE TABLE tags(id SERIAL PRIMARY KEY, %s)", cols)
}

func extractTagNamesAndTypes(tags []string) ([]string, []string) {
	tagNames := make([]string, len(tags))
	tagTypes := make([]string, len(tags))
	for i, tagWithType := range tags {
		tagAndType := strings.Split(tagWithType, " ")
		if len(tagAndType) != 2 {
			panic("tag header has invalid format")
		}
		tagNames[i] = tagAndType[0]
		tagTypes[i] = tagAndType[1]
	}

	return tagNames, tagTypes
}

// MustExec executes query or exits on error
func MustExec(db *sql.DB, query string, args ...interface{}) sql.Result {
	r, err := db.Exec(query, args...)
	if err != nil {
		fmt.Printf("could not execute sql: %s", query)
		panic(err)
	}
	return r
}

// MustQuery executes query or exits on error
func MustQuery(db *sql.DB, query string, args ...interface{}) *sql.Rows {
	r, err := db.Query(query, args...)
	if err != nil {
		panic(err)
	}
	return r
}

// MustBegin starts transaction or exits on error
func MustBegin(db *sql.DB) *sql.Tx {
	tx, err := db.Begin()
	if err != nil {
		panic(err)
	}
	return tx
}

func serializedTypeToPgType(serializedType string) string {
	switch serializedType {
	case "string":
		return "TEXT"
	case "float32":
		return "FLOAT"
	case "float64":
		return "DOUBLE PRECISION"
	case "int64":
		return "BIGINT"
	case "int32":
		return "INTEGER"
	default:
		panic(fmt.Sprintf("unrecognized type %s", serializedType))
	}
}
