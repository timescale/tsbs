package main

import (
	"bufio"
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"
	_ "github.com/mailru/go-clickhouse" // _ "github.com/mailru/go-clickhouse"
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
	defer db.Close()
	sql := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", dbName)
	_, err := db.Exec(sql)
	if err != nil {
		panic(err)
	}

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

	// d.Cols content are lines (metrics descriptions) as:
	// cpu,usage_user,usage_system,usage_idle,usage_nice,usage_iowait,usage_irq,usage_softirq,usage_steal,usage_guest,usage_guest_nice
	// disk,total,free,used,used_percent,inodes_total,inodes_free,inodes_used
	// nginx,accepts,active,handled,reading,requests,waiting,writing
	// generalised description:
	// tableName,fieldName1,...,fieldNameX

	// cpu content:
	// cpu,usage_user,usage_system,usage_idle,usage_nice,usage_iowait,usage_irq,usage_softirq,usage_steal,usage_guest,usage_guest_nice
	cpu_content := d.cols[0]
	cpu_infos := strings.Split(strings.TrimSpace(cpu_content), ",")
	cpuMetricNames := []string{}
	cpuMetricNames = append(cpuMetricNames, cpu_infos[1:]...)
	cpuMetricTypes := make([]string, len(cpuMetricNames))

	for i := 0; i < len(cpuMetricNames); i++ {
		cpuMetricTypes[i] = "Nullable(Float64)"
	}

	createCpuTagMetricTable(db, cpuMetricNames, tagNames, tagTypes)

	tagCols["cpu_tags_metrics"] = tagNames
	tagColumnTypes = tagTypes
	metricCols["cpu_tags_metrics"] = cpuMetricNames
	tableCols["cpu_tags_metrics"] = append(tagNames, cpuMetricNames...)
	tableColumnTypes["cpu_tags_metrics"] = append(tagTypes, cpuMetricTypes...)

	return nil
}

func createCpuTagMetricTable(db *sqlx.DB, metricColNames []string, tagNames, tagTypes []string) {
	localTableQuery := generateCreateTableQuery(metricColNames, tagNames, tagTypes)
	if debug > 0 {
		fmt.Println(localTableQuery)
	}

	_, err := db.Exec(localTableQuery)
	if err != nil {
		panic(err)
	}
}

func generateCreateTableQuery(metricColNames []string, tagNames, tagTypes []string) string {
	if len(tagNames) != len(tagTypes) {
		panic("wrong number of tag names and tag types")
	}

	// tags info
	tagColumnDefinitions := make([]string, len(tagNames))
	tagColumnName := make([]string, len(tagNames))
	for i, tagName := range tagNames {
		tagType := serializedTypeToClickHouseType(tagTypes[i])
		tagColumnDefinitions[i] = fmt.Sprintf("%s %s", tagName, tagType)
		tagColumnName[i] = fmt.Sprintf("%s", tagName)
	}

	tagsCols := strings.Join(tagColumnDefinitions, ",\n")
	key := strings.Join(tagColumnName, ",")

	// metricColsWithType - metricColName specifications with type. Ex.: "cpu_usage Nullable(Float64)"
	metricColsWithType := []string{}
	for _, metricColName := range metricColNames {
		if len(metricColName) == 0 {
			// Skip nameless columns
			continue
		}
		metricColsWithType = append(metricColsWithType, fmt.Sprintf("%s Nullable(Float64)", metricColName))
	}

	metricCols := strings.Join(metricColsWithType, ",\n")

	localTable := fmt.Sprintf(
		"CREATE TABLE %s.cpu_tags_metrics(\n"+
			"time DateTime DEFAULT now(),\n"+
			"%s,\n"+
			"%s"+
			") ENGINE = MergeTree() PARTITION BY toYYYYMM(time) ORDER BY (%s)",
		loader.DBName,
		tagsCols,
		metricCols,
		key)

	return localTable
}

// getConnectString() builds HTTP/TCP connect string to ClickHouse
// db - whether database specification should be added to the connection string
func getConnectString(db bool) string {
	// ClickHouse ex.:
	// http://default:passwd@127.0.0.1:8123/default
	if useHTTP {
		if db {
			fmt.Sprintf("http://%s:%s@%s:8123/%s", user, password, host, loader.DatabaseName())
		}
		return fmt.Sprintf("http://%s:%s@%s:8123", user, password, host)
	}

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
		return "LowCardinality(String)"
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
