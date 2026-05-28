package doris

import (
	"fmt"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"github.com/timescale/tsbs/pkg/targets"
)

// loader.DBCreator interface implementation
type dbCreator struct {
	ds      targets.DataSource
	headers *common.GeneratedDataHeaders
	connStr string
	config  *DorisConfig
}

// Init loader.DBCreator interface implementation
func (d *dbCreator) Init() {
	// fills dbCreator struct with data structure (tables description)
	// specified at the beginning of the data file
	d.headers = d.ds.Headers()
}

// DBExists loader.DBCreator interface implementation
func (d *dbCreator) DBExists(dbName string) bool {
	db := sqlx.MustConnect(dbType, getConnectString(d.config, false))
	defer db.Close()

	const sql = `SELECT SCHEMA_NAME 
                   FROM information_schema.SCHEMATA 
                   WHERE SCHEMA_NAME = ?`

	if d.config.Debug > 0 {
		fmt.Printf("[DEBUG] Checking database existence: %s\n", dbName+" SQL: "+sql)
	}

	var exists bool
	// 直接查询是否存在目标数据库
	err := db.Get(&exists, `SELECT EXISTS(SELECT 1 FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = ?)`, dbName)
	if err != nil {
		panic(err)
	}

	return exists
}

// RemoveOldDB loader.DBCreator interface implementation
func (d *dbCreator) RemoveOldDB(dbName string) error {
	db := sqlx.MustConnect(dbType, getConnectString(d.config, false))
	defer db.Close()

	sql := fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbName)
	if _, err := db.Exec(sql); err != nil {
		panic(err)
	}
	return nil
}

// CreateDB loader.DBCreator interface implementation
func (d *dbCreator) CreateDB(dbName string) error {
	// Connect to Doris in general and CREATE DATABASE
	db := sqlx.MustConnect(dbType, getConnectString(d.config, false))
	sql := fmt.Sprintf("CREATE DATABASE %s", dbName)
	_, err := db.Exec(sql)
	if err != nil {
		panic(err)
	}
	db.Close()
	db = nil

	// Connect to specified database within Doris
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
func createTagsTable(conf *DorisConfig, db *sqlx.DB, tagNames, tagTypes []string) {
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
func createMetricsTable(conf *DorisConfig, db *sqlx.DB, tableName string, fieldColumns []string) {
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
	for _, column := range columnNames {
		if len(column) == 0 {
			// Skip nameless columns
			continue
		}
		columnsWithType = append(columnsWithType, fmt.Sprintf("%s FLOAT", column))
	}

	sql := fmt.Sprintf(`
			CREATE TABLE %s (
				tags_id BIGINT,
				created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    			created_date DATE DEFAULT CURRENT_DATE,
				time BIGINT,
				%s,
				additional_tags String  DEFAULT '',
			    INDEX idx_created_at(created_at) USING INVERTED
			 ) DUPLICATE KEY(%s)
			DISTRIBUTED BY HASH(%s) BUCKETS 10 PROPERTIES('replication_num' = '1')
			`,
		tableName,
		strings.Join(columnsWithType, ","),
		"`tags_id`,`created_at`", "`tags_id`,`created_at`")

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
		//tagType := serializedTypeToDorisType(tagTypes[i])
		tagColumnDefinitions[i] = fmt.Sprintf("%s %s", tagName, "varchar(30)")
	}

	cols := strings.Join(tagColumnDefinitions, ",\n")

	return fmt.Sprintf(`
			CREATE TABLE tags(
			tags_id BIGINT,
			created_date DATE DEFAULT CURRENT_DATE,
    		created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			%s,
			INDEX idx_hostname(hostname) USING INVERTED,
			INDEX idx_created_at(created_at) USING INVERTED
			)
			DUPLICATE KEY(%s)
			DISTRIBUTED BY HASH(tags_id) BUCKETS 10 
			PROPERTIES("replication_num" = "1")
			`,
		cols, "`tags_id`")
}
