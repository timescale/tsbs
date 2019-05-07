package main

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/jackc/pgx"
	"log"
	"strings"
)

type tableDef struct {
	schema string
	name   string
	tags   []string
	cols   []string
}

// fqn returns the fully-qualified name of a table
func (t *tableDef) fqn() string {
	return fmt.Sprintf("\"%s\".\"%s\"", t.schema, t.name)
}

type dbCreator struct {
	tableDefs []*tableDef
	cfg       *pgx.ConnConfig
	conn      *pgx.Conn

	// common parameters for all metrics table
	numShards   int
	numReplicas int
}

// loader.DBCreator interface implementation
func (d *dbCreator) Init() {
	br := loader.GetBufferedReader()
	tableDefs, err := d.readDataHeader(br)
	if err != nil {
		fatal("cannot parse the header: %v", err)
		panic(err)
	}
	d.tableDefs = tableDefs

	conn, err := pgx.Connect(*d.cfg)
	if err != nil {
		fatal("Cannot establish a connection to database: %v", err)
		panic(err)
	}
	d.conn = conn
}

// readDataHeader fills the dbCreator struct with the data structure
// (tables description) specified at the beginning of the data file.
//
// First N lines are the header, describing the data structure.
// The first line contains the tags table name followed by a comma-separated
// list of tags:
//     tags,hostname,region,datacenter,rack,os,arch,team,service,service_version
//
// The second through N-1 line containing table name (ex.: 'disk') followed by
// list of column names, comma-separated:
//     disk,total,free,used,used_percent,inodes_total,inodes_free,inodes_used
//
// The last line being blank to separate the header from the data.
//
// Header example:
//      tags,hostname,region,datacenter,rack,os,arch,team,service,service_version,service_environment
//      disk,total,free,used,used_percent,inodes_total,inodes_free,inodes_used
//      nginx,accepts,active,handled,reading,requests,waiting,writing
func (d *dbCreator) readDataHeader(br *bufio.Reader) ([]*tableDef, error) {
	var tableDefs []*tableDef

	line, err := br.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimSpace(line)
	tagsLine := strings.Split(line, ",")
	if tagsLine[0] != "tags" {
		return nil, errors.New("first header line doesn't contain tags")
	}
	tags := tagsLine[1:]

	for {
		line, err := br.ReadString('\n')
		if err != nil {
			return nil, err
		}
		line = strings.TrimSpace(line)
		if len(line) == 0 {
			break
		}

		parts := strings.SplitN(line, ",", 2)
		if len(parts) < 2 {
			return nil, errors.New("metric columns are missing")
		}
		tableDefs = append(
			tableDefs,
			&tableDef{
				name: parts[0],
				tags: tags,
				cols: strings.Split(parts[1], ","),
			},
		)
	}
	return tableDefs, nil
}

func (d *dbCreator) CreateDB(dbName string) error {
	for _, tableDef := range d.tableDefs {
		// the dbName(schema) is required by the load.Processor implementation,
		// therefore, it is assigned to a table definition for the further usage
		tableDef.schema = dbName
		err := d.createMetricsTable(tableDef)
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *dbCreator) createMetricsTable(table *tableDef) error {
	var tagsObjectChildCols []string
	for _, column := range table.tags {
		tagsObjectChildCols = append(
			tagsObjectChildCols,
			fmt.Sprintf("%s %s", column, "string"))
	}

	var metricCols []string
	for _, column := range table.cols {
		metricCols = append(
			metricCols,
			fmt.Sprintf("%s %s", column, "double"))
	}

	// TODO partition table by configurable time interval
	sql := fmt.Sprintf(`
		CREATE TABLE %s (
			tags object as (%s),
			ts timestamp,
			%s
		) CLUSTERED INTO %d SHARDS
		WITH (number_of_replicas = %d)`,
		table.fqn(),
		strings.Join(tagsObjectChildCols, ", "),
		strings.Join(metricCols, ", "),
		d.numShards,
		d.numReplicas)
	_, err := d.conn.Exec(sql)
	if err != nil {
		return err
	}
	return nil
}

// loader.DBCreator interface implementation
//
// returns true if there are any tables in a schema
func (d *dbCreator) DBExists(dbName string) bool {
	var exists bool
	err := d.conn.QueryRow(`
		SELECT count(table_name) > 0 
		FROM information_schema.tables 
		WHERE table_schema = $1`, dbName,
	).Scan(&exists)
	if err != nil {
		fatal("cannot fetch tables for a give schema: %v", err)
		panic(err)
	}
	return exists
}

// loader.DBCreator interface implementation
func (d *dbCreator) RemoveOldDB(dbName string) error {
	tables, err := d.getTables(dbName)
	if err != nil {
		return err
	}
	for _, table := range tables {
		_, err := d.conn.Exec(fmt.Sprintf("DROP TABLE %s", table.fqn()))
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *dbCreator) getTables(dbName string) ([]tableDef, error) {
	rows, err := d.conn.Query(`
		SELECT table_schema, table_name
		FROM information_schema.tables
		WHERE table_schema = $1`, dbName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []tableDef
	for rows.Next() {
		var t tableDef
		err := rows.Scan(&t.schema, &t.name)
		if err != nil {
			return nil, err
		}
		tables = append(tables, t)
	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}
	return tables, nil
}

func (d *dbCreator) Close() {
	if err := d.conn.Close(); err != nil {
		log.Printf("an error on connection closing: %v", err)
	}
}
