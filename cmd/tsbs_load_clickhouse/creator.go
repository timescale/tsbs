package main

import (
	"bufio"
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"
)

// loader.DBCreator interface implementation
type dbCreator struct {
	tags    string
	cols    []string
	connStr string
}

// loader.DBCreator interface implementation
func (d *dbCreator) Init() {
	br := loader.GetBufferedReader()
	d.readDataHeader(br)
}

// readDataHeader fills dbCreator struct with data structure (tables description)
// specified at the beginning of the data file
func (d *dbCreator) readDataHeader(br *bufio.Reader) {
	// First N lines are header, describing data structure.
	// The first line containing tags table name ('tags') followed by list of tags, comma-separated.
	// Ex.: tags,hostname,region,datacenter,rack,os,arch,team,service,service_version
	// The second through N-1 line containing table name (ex.: 'cpu') followed by list of column names,
	// comma-separated. Ex.: cpu,usage_user,usage_system,usage_idle,usage_nice,usage_iowait,usage_irq,usage_softirq
	// The last line being blank to separate from the data
	//
	// Header example:
	// tags,hostname,region,datacenter,rack,os,arch,team,service,service_version,service_environment
	// cpu,usage_user,usage_system,usage_idle,usage_nice,usage_iowait,usage_irq,usage_softirq,usage_steal,usage_guest,usage_guest_nice
	// disk,total,free,used,used_percent,inodes_total,inodes_free,inodes_used
	// nginx,accepts,active,handled,reading,requests,waiting,writing

	i := 0
	for {
		var err error
		var line string

		if i == 0 {
			// read first line - list of tags
			d.tags, err = br.ReadString('\n')
			if err != nil {
				fatal("input has wrong header format: %v", err)
			}
			d.tags = strings.TrimSpace(d.tags)
		} else {
			// read the second and further lines - metrics descriptions
			line, err = br.ReadString('\n')
			if err != nil {
				fatal("input has wrong header format: %v", err)
			}
			line = strings.TrimSpace(line)
			if len(line) == 0 {
				// empty line - end of header
				break
			}
			// append new table/columns set to the list of tables/columns set
			d.cols = append(d.cols, line)
		}
		i++
	}
}

// loader.DBCreator interface implementation
func (d *dbCreator) DBExists(dbName string) bool {
	db := sqlx.MustConnect(dbType, getConnectString(false))
	defer db.Close()

	sql := fmt.Sprintf("SELECT name, engine FROM system.databases WHERE name = '%s'", dbName)
	if debug > 0 {
		fmt.Printf(sql)
	}
	var rows []struct {
		Name   string `db:"name"`
		Engine string `db:"engine"`
	}

	err := db.Select(&rows, sql)
	if err != nil {
		panic(err)
	}
	for _, row := range rows {
		if row.Name == dbName {
			return true
		}
	}

	return false
}

// loader.DBCreator interface implementation
func (d *dbCreator) RemoveOldDB(dbName string) error {
	db := sqlx.MustConnect(dbType, getConnectString(false))
	defer db.Close()

	sql := fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbName)
	if _, err := db.Exec(sql); err != nil {
		panic(err)
	}
	return nil
}

// loader.DBCreator interface implementation
func (d *dbCreator) CreateDB(dbName string) error {
	// Connect to ClickHouse in general and CREATE DATABASE
	db := sqlx.MustConnect(dbType, getConnectString(false))
	sql := fmt.Sprintf("CREATE DATABASE %s", dbName)
	_, err := db.Exec(sql)
	if err != nil {
		panic(err)
	}
	db.Close()
	db = nil

	// Connect to specified database within ClickHouse
	db = sqlx.MustConnect(dbType, getConnectString(true))
	defer db.Close()

	// d.tags content:
	//tags,hostname,region,datacenter,rack,os,arch,team,service,service_version,service_environment
	//
	// Parts would contain
	// 0: tags - reserved word - tags mark
	// 1:
	// N: actual tags
	// so we'll use tags[1:] for tags specification
	parts := strings.Split(strings.TrimSpace(d.tags), ",")
	if parts[0] != "tags" {
		return fmt.Errorf("input header in wrong format. got '%s', expected 'tags'", parts[0])
	}
	tagNames, tagTypes := extractTagNamesAndTypes(parts[1:])
	createTagsTable(db, tagNames, tagTypes)
	tableCols["tags"] = tagNames
	tagColumnTypes = tagTypes

	// d.cols content are lines (metrics descriptions) as:
	// cpu,usage_user,usage_system,usage_idle,usage_nice,usage_iowait,usage_irq,usage_softirq,usage_steal,usage_guest,usage_guest_nice
	// disk,total,free,used,used_percent,inodes_total,inodes_free,inodes_used
	// nginx,accepts,active,handled,reading,requests,waiting,writing
	// generalised description:
	// tableName,fieldName1,...,fieldNameX
	for _, cols := range d.cols {
		// cols content:
		// cpu,usage_user,usage_system,usage_idle,usage_nice,usage_iowait,usage_irq,usage_softirq,usage_steal,usage_guest,usage_guest_nice
		createMetricsTable(db, strings.Split(strings.TrimSpace(cols), ","))
	}

	return nil
}

// createTagsTable builds CREATE TABLE SQL statement and runs it
func createTagsTable(db *sqlx.DB, tagNames, tagTypes []string) {
	sql := generateTagsTableQuery(tagNames, tagTypes)
	if debug > 0 {
		fmt.Printf(sql)
	}
	_, err := db.Exec(sql)
	if err != nil {
		panic(err)
	}
}

func generateTagsTableQuery(tagNames, tagTypes []string) string {
	// prepare COLUMNs specification for CREATE TABLE statement
	// all columns would be of the type specified in the tags header
	// e.g. tags, tag2 string,tag2 int32...
	tagColumnDefinitions := make([]string, len(tagNames))
	for i, tagName := range tagNames {
		tagType := serializedTypeToClickHouseType(tagTypes[i])
		tagColumnDefinitions[i] = fmt.Sprintf("%s %s", tagName, tagType)
	}

	cols := strings.Join(tagColumnDefinitions, ",\n")

	index := "id"

	return fmt.Sprintf(
		"CREATE TABLE tags(\n"+
			"created_date Date     DEFAULT today(),\n"+
			"created_at   DateTime DEFAULT now(),\n"+
			"id           UInt32,\n"+
			"%s"+
			") ENGINE = MergeTree(created_date, (%s), 8192)",
		cols,
		index)
}

// createMetricsTable builds CREATE TABLE SQL statement and runs it
func createMetricsTable(db *sqlx.DB, tableSpec []string) {
	// tableSpec contain
	// 0: table name
	// 1: table column name 1
	// N: table column name N

	// Ex.: cpu OR disk OR nginx
	tableName := tableSpec[0]
	tableCols[tableName] = tableSpec[1:]

	// We'll have some service columns in table to be created and columnNames contains all column names to be created
	columnNames := []string{}

	if inTableTag {
		// First column in the table - service column - partitioning field
		partitioningColumn := tableCols["tags"][0] // would be 'hostname'
		columnNames = append(columnNames, partitioningColumn)
	}

	// Add all column names from tableSpec into columnNames
	columnNames = append(columnNames, tableSpec[1:]...)

	// columnsWithType - column specifications with type. Ex.: "cpu_usage Float64"
	columnsWithType := []string{}
	for _, column := range columnNames {
		if len(column) == 0 {
			// Skip nameless columns
			continue
		}
		columnsWithType = append(columnsWithType, fmt.Sprintf("%s Nullable(Float64)", column))
	}

	sql := fmt.Sprintf(`
			CREATE TABLE %s (
				created_date    Date     DEFAULT today(),
				created_at      DateTime DEFAULT now(),
				time            String,
				tags_id         UInt32,
				%s,
				additional_tags String   DEFAULT ''
			) ENGINE = MergeTree(created_date, (tags_id, created_at), 8192)
			`,
		tableName,
		strings.Join(columnsWithType, ","))
	if debug > 0 {
		fmt.Printf(sql)
	}
	_, err := db.Exec(sql)
	if err != nil {
		panic(err)
	}
}

// getConnectString() builds connect string to ClickHouse
// db - whether database specification should be added to the connection string
func getConnectString(db bool) string {
	// connectString: tcp://127.0.0.1:9000?debug=true
	// ClickHouse ex.:
	// tcp://host1:9000?username=user&password=qwerty&database=clicks&read_timeout=10&write_timeout=20&alt_hosts=host2:9000,host3:9000
	if db {
		return fmt.Sprintf("tcp://%s:9000?username=%s&password=%s&database=%s", host, user, password, loader.DatabaseName())
	}

	return fmt.Sprintf("tcp://%s:9000?username=%s&password=%s", host, user, password)
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

func serializedTypeToClickHouseType(serializedType string) string {
	switch serializedType {
	case "string":
		return "Nullable(String)"
	case "float32":
		return "Nullable(Float32)"
	case "float64":
		return "Nullable(Float64)"
	case "int64":
		return "Nullable(Int64)"
	case "int32":
		return "Nullable(Int32)"
	default:
		panic(fmt.Sprintf("unrecognized type %s", serializedType))
	}
}
