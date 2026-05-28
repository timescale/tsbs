package clickhouse

import (
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"
	_ "github.com/kshvakov/clickhouse"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"github.com/timescale/tsbs/pkg/targets"
)

// loader.DBCreator interface implementation
type dbCreator struct {
	ds      targets.DataSource
	headers *common.GeneratedDataHeaders
	connStr string
	config  *ClickhouseConfig
}

// loader.DBCreator interface implementation
func (d *dbCreator) Init() {
	// fills dbCreator struct with data structure (tables description)
	// specified at the beginning of the data file
	d.headers = d.ds.Headers()
}

// loader.DBCreator interface implementation
func (d *dbCreator) DBExists(dbName string) bool {
	db := sqlx.MustConnect(dbType, getConnectString(d.config, false))
	defer db.Close()

	sql := fmt.Sprintf("SELECT name, engine FROM system.databases WHERE name = '%s'", dbName)
	if d.config.Debug > 0 {
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
	db := sqlx.MustConnect(dbType, getConnectString(d.config, false))
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
	db := sqlx.MustConnect(dbType, getConnectString(d.config, false))
	sql := fmt.Sprintf("CREATE DATABASE %s", dbName)
	_, err := db.Exec(sql)
	if err != nil {
		panic(err)
	}
	db.Close()
	db = nil

	// Connect to specified database within ClickHouse
	db = sqlx.MustConnect(dbType, getConnectString(d.config, true))
	defer db.Close()

	createTagsTable(d.config, db, d.headers.TagKeys, d.headers.TagTypes)
	if tableCols == nil {
		tableCols = make(map[string][]string)
	}
	tableCols["tags"] = d.headers.TagKeys
	tagColumnTypes = d.headers.TagTypes

	for tableName, fieldColumns := range d.headers.FieldKeys {
		//tableName: cpu
		// fieldColumns content:
		// usage_user,usage_system,usage_idle,usage_nice,usage_iowait,usage_irq,usage_softirq,usage_steal,usage_guest,usage_guest_nice
		createMetricsTable(d.config, db, tableName, fieldColumns)
	}

	return nil
}

// createTagsTable builds CREATE TABLE SQL statement and runs it
func createTagsTable(conf *ClickhouseConfig, db *sqlx.DB, tagNames, tagTypes []string) {
	sql := generateTagsTableQuery(tagNames, tagTypes)
	if conf.Debug > 0 {
		fmt.Printf(sql)
	}
	_, err := db.Exec(sql)
	if err != nil {
		panic(err)
	}
}

// createMetricsTable builds CREATE TABLE SQL statement and runs it
func createMetricsTable(conf *ClickhouseConfig, db *sqlx.DB, tableName string, fieldColumns []string) {
	tableCols[tableName] = fieldColumns

	// We'll have some service columns in table to be created and columnNames contains all column names to be created
	var columnNames []string

	if conf.InTableTag {
		// First column in the table - service column - partitioning field
		partitioningColumn := tableCols["tags"][0] // would be 'hostname'
		columnNames = append(columnNames, partitioningColumn)
	}

	// Add all column names from fieldColumns into columnNames
	columnNames = append(columnNames, fieldColumns...)

	// columnsWithType - column specifications with type. Ex.: "cpu_usage Float64"
	var columnsWithType []string
	for idx, column := range columnNames {
		if len(column) == 0 {
			// Skip nameless columns
			continue
		}
		if conf.InTableTag && idx == 0 {
			columnsWithType = append(columnsWithType, fmt.Sprintf("%s Nullable(String)", column))
		} else {
			columnsWithType = append(columnsWithType, fmt.Sprintf("%s Nullable(Float64)", column))
		}

	}

	sql := fmt.Sprintf(`
			CREATE TABLE %s (
				created_date    Date     DEFAULT today(),
				created_at      DateTime DEFAULT now(),
				time            String,
				tags_id         UInt32,
				%s,
				additional_tags String   DEFAULT ''
			) ENGINE = MergeTree() PARTITION BY toYYYYMM(created_date) PRIMARY KEY (tags_id, created_at)
			`,
		tableName,
		strings.Join(columnsWithType, ","))
	if conf.Debug > 0 {
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
	if len(tagNames) != len(tagTypes) {
		panic("wrong number of tag names and tag types")
	}

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
			") ENGINE = MergeTree()"+
			"PARTITION BY toYYYYMM(created_date) PRIMARY KEY %s",
		cols,
		index)
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
