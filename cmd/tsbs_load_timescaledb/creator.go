package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"regexp"
	"strings"

	_ "github.com/jackc/pgx/v4/stdlib"
)

const tagsKey = "tags"

var tableCols = make(map[string][]string)

type dbCreator struct {
	br      *bufio.Reader
	tags    string
	cols    []string
	connStr string
	connDB  string
}

func (d *dbCreator) Init() {
	d.readDataHeader(d.br)
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

func (d *dbCreator) readDataHeader(br *bufio.Reader) {
	// First N lines are header, with the first line containing the tags
	// and their names, the second through N-1 line containing the column
	// names, and last line being blank to separate from the data
	i := 0
	for {
		var err error
		var line string
		if i == 0 {
			d.tags, err = br.ReadString('\n')
			if err != nil {
				fatal("input has wrong header format: %v", err)
			}
			d.tags = strings.TrimSpace(d.tags)
		} else {
			line, err = br.ReadString('\n')
			if err != nil {
				fatal("input has wrong header format: %v", err)
			}
			line = strings.TrimSpace(line)
			if len(line) == 0 {
				break
			}
			d.cols = append(d.cols, line)
		}
		i++
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

// MustExec executes query or exits on error
func MustExec(db *sql.DB, query string, args ...interface{}) sql.Result {
	r, err := db.Exec(query, args...)
	if err != nil {
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

func (d *dbCreator) DBExists(dbName string) bool {
	db := MustConnect(driver, d.connStr)
	defer db.Close()
	r := MustQuery(db, "SELECT 1 from pg_database WHERE datname = $1", dbName)
	defer r.Close()
	return r.Next()
}

func (d *dbCreator) RemoveOldDB(dbName string) error {
	db := MustConnect(driver, d.connStr)
	defer db.Close()
	MustExec(db, "DROP DATABASE IF EXISTS "+dbName)
	return nil
}

func (d *dbCreator) CreateDB(dbName string) error {
	db := MustConnect(driver, d.connStr)
	MustExec(db, "CREATE DATABASE "+dbName)
	db.Close()
	return nil
}

func (d *dbCreator) PostCreateDB(dbName string) error {
	dbBench := MustConnect(driver, getConnectString())
	defer dbBench.Close()

	tags := strings.Split(strings.TrimSpace(d.tags), ",")
	if tags[0] != tagsKey {
		return fmt.Errorf("input header in wrong format. got '%s', expected 'tags'", tags[0])
	}
	tagNames, tagTypes := extractTagNamesAndTypes(tags[1:])
	if createMetricsTable {
		createTagsTable(dbBench, tagNames, tagTypes)
	}
	// tableCols is a global map. Globally cache the available tags
	tableCols[tagsKey] = tagNames
	// tagTypes holds the type of each tag value (as strings from Go types (string, float32...))
	tagColumnTypes = tagTypes

	// Each table is defined in the dbCreator 'cols' list. The definition consists of a
	// comma separated list of the table name followed by its columns. Iterate over each
	// definition to update our global cache and create the requisite tables and indexes
	for _, tableDef := range d.cols {
		columns := strings.Split(strings.TrimSpace(tableDef), ",")
		tableName := columns[0]
		// tableCols is a global map. Globally cache the available columns for the given table
		tableCols[tableName] = columns[1:]

		fieldDefs, indexDefs := d.getFieldAndIndexDefinitions(columns)
		if createMetricsTable {
			d.createTableAndIndexes(dbBench, tableName, fieldDefs, indexDefs)
		}
	}
	return nil
}

// getFieldAndIndexDefinitions iterates over a list of table columns, populating lists of
// definitions for each desired field and index. Returns separate lists of fieldDefs and indexDefs
func (d *dbCreator) getFieldAndIndexDefinitions(columns []string) ([]string, []string) {
	var fieldDefs []string
	var indexDefs []string
	var allCols []string

	partitioningField := tableCols[tagsKey][0]
	tableName := columns[0]
	// If the user has specified that we should partition on the primary tags key, we
	// add that to the list of columns to create
	if inTableTag {
		allCols = append(allCols, partitioningField)
	}

	allCols = append(allCols, columns[1:]...)
	extraCols := 0 // set to 1 when hostname is kept in-table
	for idx, field := range allCols {
		if len(field) == 0 {
			continue
		}
		fieldType := "DOUBLE PRECISION"
		idxType := fieldIndex
		// This condition handles the case where we keep the primary tag key in the table
		// and partition on it. Since under the current implementation this tag is always
		// hostname, we set it to a TEXT field instead of DOUBLE PRECISION
		if inTableTag && idx == 0 {
			fieldType = "TEXT"
			idxType = ""
			extraCols = 1
		}

		fieldDefs = append(fieldDefs, fmt.Sprintf("%s %s", field, fieldType))
		// If the user specifies indexes on additional fields, add them to
		// our index definitions until we've reached the desired number of indexes
		if fieldIndexCount == -1 || idx < (fieldIndexCount+extraCols) {
			indexDefs = append(indexDefs, d.getCreateIndexOnFieldCmds(tableName, field, idxType)...)
		}
	}
	return fieldDefs, indexDefs
}

// createTableAndIndexes takes a list of field and index definitions for a given tableName and constructs
// the necessary table, index, and potential hypertable based on the user's settings
func (d *dbCreator) createTableAndIndexes(dbBench *sql.DB, tableName string, fieldDefs []string, indexDefs []string) {
	MustExec(dbBench, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	MustExec(dbBench, fmt.Sprintf("CREATE TABLE %s (time timestamptz, tags_id integer, %s, additional_tags JSONB DEFAULT NULL)", tableName, strings.Join(fieldDefs, ",")))
	if partitionIndex {
		MustExec(dbBench, fmt.Sprintf("CREATE INDEX ON %s(tags_id, \"time\" DESC)", tableName))
	}

	// Only allow one or the other, it's probably never right to have both.
	// Experimentation suggests (so far) that for 100k devices it is better to
	// use --time-partition-index for reduced index lock contention.
	if timePartitionIndex {
		MustExec(dbBench, fmt.Sprintf("CREATE INDEX ON %s(\"time\" DESC, tags_id)", tableName))
	} else if timeIndex {
		MustExec(dbBench, fmt.Sprintf("CREATE INDEX ON %s(\"time\" DESC)", tableName))
	}

	for _, indexDef := range indexDefs {
		MustExec(dbBench, indexDef)
	}

	if useHypertable {
		MustExec(dbBench, "CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE")
		MustExec(dbBench,
			fmt.Sprintf("SELECT create_hypertable('%s'::regclass, 'time'::name, partitioning_column => '%s'::name, number_partitions => %v::smallint, chunk_time_interval => %d, create_default_indexes=>FALSE)",
				tableName, "tags_id", numberPartitions, chunkTime.Nanoseconds()/1000))
	}
}

func (d *dbCreator) getCreateIndexOnFieldCmds(hypertable, field, idxType string) []string {
	ret := []string{}
	for _, idx := range strings.Split(idxType, ",") {
		if idx == "" {
			continue
		}

		indexDef := ""
		if idx == timeValueIdx {
			indexDef = fmt.Sprintf("(time DESC, %s)", field)
		} else if idx == valueTimeIdx {
			indexDef = fmt.Sprintf("(%s, time DESC)", field)
		} else {
			fatal("Unknown index type %v", idx)
		}

		ret = append(ret, fmt.Sprintf("CREATE INDEX ON %s %s", hypertable, indexDef))
	}
	return ret
}

func createTagsTable(db *sql.DB, tagNames, tagTypes []string) {
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
