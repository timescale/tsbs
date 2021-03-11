package main

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/jackc/pgx/v4"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"github.com/timescale/tsbs/pkg/targets"
)

type tableDef struct {
	schema   string
	name     string
	tags     []string
	tagTypes []string
	cols     []string
}

// fqn returns the fully-qualified name of a table
func (t *tableDef) fqn() string {
	return fmt.Sprintf("\"%s\".\"%s\"", t.schema, t.name)
}

type dbCreator struct {
	tableDefs []*tableDef
	cfg       *pgx.ConnConfig
	conn      *pgx.Conn
	ds        targets.DataSource
	// common parameters for all metrics table
	numShards   int
	numReplicas int
}

// loader.DBCreator interface implementation
func (d *dbCreator) Init() {
	header := d.ds.Headers()
	tableDefs, err := d.readDataHeader(header)
	if err != nil {
		fatal("cannot parse the header: %v", err)
		panic(err)
	}
	d.tableDefs = tableDefs

	conn, err := pgx.ConnectConfig(context.Background(), d.cfg)
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
func (d *dbCreator) readDataHeader(header *common.GeneratedDataHeaders) ([]*tableDef, error) {
	var tableDefs []*tableDef
	for tableName, fieldCols := range header.FieldKeys {
		tableDefs = append(
			tableDefs,
			&tableDef{
				name:     tableName,
				tags:     header.TagKeys,
				tagTypes: header.TagTypes,
				cols:     fieldCols,
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
	for i, column := range table.tags {
		if table.tagTypes[i] != "string" {
			return fmt.Errorf("cratedb db creator does not support non-string tags")
		}
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
	_, err := d.conn.Exec(context.Background(), sql)
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
	err := d.conn.QueryRow(context.Background(), `
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
		_, err := d.conn.Exec(context.Background(), fmt.Sprintf("DROP TABLE %s", table.fqn()))
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *dbCreator) getTables(dbName string) ([]tableDef, error) {
	rows, err := d.conn.Query(context.Background(), `
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
	if err := d.conn.Close(context.Background()); err != nil {
		log.Printf("an error on connection closing: %v", err)
	}
}
